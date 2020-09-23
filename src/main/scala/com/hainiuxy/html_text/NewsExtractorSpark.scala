package com.hainiuxy.html_text

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.sql.{DriverManager, PreparedStatement, Statement, Timestamp, Connection => SqlConnection}
import java.{sql, util}
import java.util.Date

import com.hainiuxy.html_text.acc.MapAccumulator
import com.hainiuxy.html_text.config.Constants
import com.hainiuxy.html_text.redis_real.HainiuJedisConnectionPool
import com.hainiuxy.html_text.utils.{JavaUtil, Util}
import com.hainiuxy.html_text.utils.extractor.HtmlContentExtractor
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import redis.clients.jedis.{Jedis, Transaction}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object NewsExtractorSpark {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("NewExtractorSpark")

    // 设置es的节点
    conf.set("es.nodes", Constants.ES_NODE_NAME)
    // 设置es通信端口
    conf.set("es.port", "9200")

    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("/tmp/spark/news_input")

    //创建5个自定义累加器, 用来记录每个批次中每个host的抽取情况
    val hostScanAccumulator = new MapAccumulator
    val hostFilteredAccumulator = new MapAccumulator
    val hostExtractAccumulator = new MapAccumulator
    val hostEmptyAccumulator = new MapAccumulator
    val hostNoMatchAccumulator = new MapAccumulator
    sc.register(hostScanAccumulator)
    sc.register(hostFilteredAccumulator)
    sc.register(hostExtractAccumulator)
    sc.register(hostEmptyAccumulator)
    sc.register(hostNoMatchAccumulator)

    import org.elasticsearch.spark._

    // 定义广播变量                       host                  正反xpath规则            人工和机器xpath
    var broad: Broadcast[mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]] = sc.broadcast(new mutable.HashMap[String ,mutable.HashMap[String, mutable.HashSet[String]]])

    // 更新时间间隔 10s, 每隔10秒钟跟新一次
    val updateInerval:Long = 10000

    // 广播变量最后一次更新时间
    var lastUpdateTime:Long = 0L

    if(broad.value.isEmpty){
      val configMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]()

      // 加载广播变量
      val fs: FileSystem = FileSystem.get(new Configuration())
      val files: Array[FileStatus] = fs.listStatus(new Path(Constants.CONFIG_PATH))
      for(file <- files){
        val path: Path = file.getPath()
        // 用hdfs的读取方式写，不能用FileInputStream
        val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
        var line = ""
        line = reader.readLine()
        while(line != null){
          val arr: Array[String] = line.split("\t")

          breakable(
            if(arr.length != 4){
              break()
            }else{
              val host: String = arr(0)
              val xpath: String = arr(1)
              // 正反xpath
              val tfXpathType: String = arr(2)

              // host对应的xpathMap        正反xpath          xpath
              val xpathMap: mutable.HashMap[String, mutable.HashSet[String]] = configMap.getOrElseUpdate(host, new mutable.HashMap[String, mutable.HashSet[String]]())

              val set: mutable.HashSet[String] = xpathMap.getOrElseUpdate(tfXpathType, new mutable.HashSet[String])

              set.add(xpath)

            }
          )
          line = reader.readLine()

        }
        reader.close()
      }

      println(configMap)

      // 卸载广播变量
      broad.unpersist()

      // 更新广播变量
      broad = sc.broadcast(configMap)

      // 更新 广播变量最后的更新时间
      lastUpdateTime = System.currentTimeMillis()
      println(s"更新广播变量结束， ${lastUpdateTime}")
    }

    // 筛选
    val filterRdd: RDD[String] = rdd.filter(line => {
      val splits: Array[String] = line.split("\001")
      var isCorrect: Boolean = true

      if (splits.length == 3) {
        val md5 = splits(0)
        val url = splits(1)
        val html = splits(2)
        val urlT = new URL(url)
        val host: String = urlT.getHost
        hostScanAccumulator.add(host -> 1L)
        val checkMd5 = DigestUtils.md5Hex(s"$url\001$html")
        if (!checkMd5.equals(md5)) {
          hostFilteredAccumulator.add(host -> 1L)
          isCorrect = false
        }
      } else if (splits.length == 2) {
        val url = splits(1)
        val urlT = new URL(url)
        val host: String = urlT.getHost
        hostScanAccumulator.add(host -> 1L)
        hostFilteredAccumulator.add(host -> 1L)
        isCorrect = false
      } else {
        hostScanAccumulator.add("HOST_NULL" -> 1L)
        hostFilteredAccumulator.add("HOST_NULL" -> 1L)
        isCorrect = false
      }
      isCorrect

    })

    val needSaveESRdd: RDD[Map[String,String]] = filterRdd.mapPartitions(it => {

      // 获取广播变量里的配置
      val configMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]] = broad.value

      val hbasePuts = new ListBuffer[Put]

      val esMaps = new ListBuffer[Map[String,String]]
      it.foreach(f => {
        // 获取xpath
        val splits: Array[String] = f.split("\001")
        val url: String = splits(1)
        val html: String = splits(2)
        val host: String = new URL(url).getHost


        val xpathMap: util.Map[String, String] = HtmlContentExtractor.generateXpath(html)
        println(xpathMap)


        // 正规则xpath
        var txpath:String = ""
        // 范规则xpath数组
        val fxpaths = new ArrayBuffer[String]
        if (JavaUtil.isNotEmpty(xpathMap)) {
          val it: util.Iterator[util.Map.Entry[String, String]] = xpathMap.entrySet().iterator()
          while (it.hasNext) {
            val entry: util.Map.Entry[String, String] = it.next()
            val key = entry.getKey

            val value: String = entry.getValue
            println(s"key:${key},value:${value}")

            //正文xpath
            if(value.equals(HtmlContentExtractor.CONTENT)){
              txpath = key
            }else{
              // 反规则xpath
              fxpaths += key
            }

          }
        }

        // 写入redis
        // 创建连接
        val redis: Jedis = HainiuJedisConnectionPool.getConnection()
        if(! txpath.equals("") || fxpaths.size > 0){
          // 开启redis事务
          val transaction: Transaction = redis.multi()
          //切换数据库
          transaction.select(10)
          // 用于统计该host的xpath总数
          transaction.incr(s"panniu:total:${host}")
          if(! txpath.equals("")){
            // 用于统计该host的模板xpath对应的总数
            transaction.zincrby(s"panniu:txpath:${host}", 1d, txpath)
          }

          if(fxpaths.size > 0){
            transaction.sadd(s"panniu:fxpath:${host}", fxpaths:_*)
          }

          // 关闭redis事务
          transaction.exec()
        }

        // 关闭连接
        redis.close()

        // 提取正文
        // 提取正文是通过大数据技术累计数据，找到对应host的xpath模板
        // 通过xpath模板来提取。如果xpath模板没找到，则不提取正文。
        if(configMap.contains(host)){

          val hostXpathMap: mutable.HashMap[String, mutable.HashSet[String]] = configMap.getOrElseUpdate(host, new mutable.HashMap[String, mutable.HashSet[String]])
          // 正规则xpath set
          val tset: mutable.HashSet[String] = hostXpathMap.getOrElseUpdate("true", new mutable.HashSet[String]())
          // 反规则xpath set
          val fset: mutable.HashSet[String] = hostXpathMap.getOrElseUpdate("false", new mutable.HashSet[String]())

          println(s"正规则xpath：${tset}")
          println(s"反规则xpath：${fset}")

          var content:String = ""
          try{
            val doc: Document = Jsoup.parse(html)
            // 正文提取
            content = Util.getContext(doc, tset, fset)
            println(s"content:${content}")
          }catch {
            case e:Exception => content = ""
          }

          if(content.trim.length >= 10){

            // 生产put对象，装到list里

            val time: Long = new Date().getTime
            // es写入时间格式化
            val esTime = Util.getTime(time, "yyyy-MM-dd HH:mm:ss")

            val putTime = Util.getTime(time, "yyyyMMddHHmmssSSSS")
            val urlmd5 = DigestUtils.md5Hex(s"$url")
            val domain: String = JavaUtil.getDomainName(new URL(url))

            // rowkey: host_time_urlmd5
            val put = new Put(Bytes.toBytes(s"${host}_${putTime}_${urlmd5}"))
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("url"),Bytes.toBytes(s"${url}"))
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("domain"),Bytes.toBytes(s"${domain}"))
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("host"),Bytes.toBytes(s"${host}"))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("content"),Bytes.toBytes(s"${content}"))
            put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("html"),Bytes.toBytes(s"${html}"))
            hbasePuts += put


            // es: url、url_md5、host、domain、date、content
            esMaps += Map("url"-> url,
              "url_md5" -> urlmd5,
              "host" -> host,
              "domain" -> domain,
              "date" -> esTime,
              "content" -> content)

            hostExtractAccumulator.add(host -> 1L)

          }else if(content.trim().size < 10){
            hostEmptyAccumulator.add(host -> 1L)
          }

        }else{
          // 此host没有匹配的xpath规则
          hostNoMatchAccumulator.add(host -> 1L)
        }


      })

      // 创建hbase连接,批量put 到 hbase
      val hbaseConf: Configuration = HBaseConfiguration.create()
      val conn: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: HTable = conn.getTable(TableName.valueOf(Constants.HASE_TABLE_NAME)).asInstanceOf[HTable]
      import scala.collection.convert.wrapAsJava._
      table.put(hbasePuts)
      table.close()
      conn.close()

      // 返回准备写入es的rdd
      esMaps.iterator
    })

    needSaveESRdd.saveToEs(s"${Constants.ES_INDEX_TYPE}")
//
//
//    // 把统计的累加器结果写入数据表，报表体现
    val scanAccMap: mutable.HashMap[Any, Any] = hostScanAccumulator.value
    val filteredAccMap: mutable.HashMap[Any, Any] = hostFilteredAccumulator.value
    val extractAccMap: mutable.HashMap[Any, Any] = hostExtractAccumulator.value
    val emptyAccMap: mutable.HashMap[Any, Any] = hostEmptyAccumulator.value
    val noMatchAccMap: mutable.HashMap[Any, Any] = hostNoMatchAccumulator.value

    if(scanAccMap.size > 0){

      val date = new Date()
      val time: Long = date.getTime
      val timestamp = new Timestamp(time)
      val hour_md5: String = DigestUtils.md5Hex(Util.getTime(time, "yyyyMMddHH"))
      val scanDay: Int = Util.getTime(time, "yyyyMMdd").toInt
      val scanHour: Int = Util.getTime(time, "HH").toInt

      var conn:SqlConnection = null
      var stmt:Statement = null

      try{
        classOf[com.mysql.jdbc.Driver]
        conn = DriverManager.getConnection(Constants.MYSQL_CONFIG("url"),
          Constants.MYSQL_CONFIG("username"),
          Constants.MYSQL_CONFIG("password"))
        conn.setAutoCommit(false)

        stmt = conn.createStatement()

        for((host, num) <- scanAccMap) {
          val scanNum: Long = num.asInstanceOf[Long]
          val filteredNum: Long = filteredAccMap.getOrElse(host, 0L).asInstanceOf[Long]
          val extractNum: Long = extractAccMap.getOrElse(host, 0L).asInstanceOf[Long]
          val emptyNum: Long = emptyAccMap.getOrElse(host, 0L).asInstanceOf[Long]
          val noMatchNum: Long = noMatchAccMap.getOrElse(host, 0L).asInstanceOf[Long]

          val sql =
            s"""
               |insert into report_stream_extract
               |(host, scan, filtered, extract, empty_context, no_match_xpath, scan_day, scan_hour, scan_time, hour_md5)
               |values('$host', $scanNum, $filteredNum, $extractNum, $emptyNum, $noMatchNum, $scanDay, $scanHour, '$timestamp','$hour_md5')
               |on DUPLICATE KEY UPDATE
               |scan=scan+$scanNum, filtered=filtered+$filteredNum, extract=extract+$extractNum,
               |empty_context=empty_context+$emptyNum, no_match_xpath=no_match_xpath+$noMatchNum;
            """.stripMargin

          stmt.execute(sql)

        }
        conn.commit()
      }catch {
        case e:Exception => {
          e.printStackTrace()
          try{
            conn.rollback()
          }catch{
            case e:Exception => e.printStackTrace()
          }
        }
      }finally {
        try{
          if(stmt != null) stmt.close()
          if(conn != null) conn.close()
        }catch{
          case e:Exception => e.printStackTrace()
        }
      }
    }

    // 重置累加器
    hostScanAccumulator.reset()
    hostFilteredAccumulator.reset()
    hostExtractAccumulator.reset()
    hostEmptyAccumulator.reset()
    hostNoMatchAccumulator.reset()

  }
}
