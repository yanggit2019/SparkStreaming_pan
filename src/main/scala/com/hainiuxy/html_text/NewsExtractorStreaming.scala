package com.hainiuxy.html_text

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.sql.{DriverManager, Statement, Timestamp}
import java.sql.{Connection => SqlConnection}
import java.util
import java.util.Date

import com.hainiuxy.html_text.acc.MapAccumulator
import com.hainiuxy.html_text.config.Constants
import com.hainiuxy.html_text.redis_real.HainiuJedisConnectionPool
import com.hainiuxy.html_text.utils.extractor.HtmlContentExtractor
import com.hainiuxy.html_text.utils.{JavaUtil, Util}
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi.{OffsetRequest, PartitionMetadata, TopicMetadataRequest, TopicMetadataResponse}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import redis.clients.jedis.{Jedis, Transaction}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.util.control.Breaks.{break, breakable}
class NewsExtractorStreaming
object NewsExtractorStreaming {
  def main(args: Array[String]): Unit = {
    //指定组名
    val group = Constants.KAFKA_CONSUMER_GROUP
    //创建SparkConf
    val conf = new SparkConf().setAppName("NewsExtractorStreaming")
      .setMaster("local[*]")

    // 设置es的节点
    conf.set("es.nodes", Constants.ES_NODE_NAME)
    // 设置es通信端口
    conf.set("es.port", "9200")

    //创建SparkStreaming，设置间隔时间
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    //指定 topic 名字
    val topic = Constants.KAFKA_CONFIG_MAP("topic")

    //SparkStreaming时使用的topic集合，可同时消费多个topic
    val topics: Set[String] = Set(topic)
    //topic在zk里的数据路径，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //得到zk中的数据路径 例如："/consumers/${group}/offsets/${topic}"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> Constants.KAFKA_CONFIG_MAP("brokers"),
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "latest"
    )

    //定义一个空的kafkaStream，之后根据是否有历史的偏移量进行选择
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null

    //创建zk客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(Constants.KAFKA_CONFIG_MAP("zk_client"))

    //如果存在历史的偏移量，那使用fromOffsets来存放存储在zk中的每个TopicPartition对应的offset
    // 是外部存储zookeeper存的offset
    var zkOffsets = new HashMap[TopicPartition, Long]

    //从zk中查询该数据路径下是否有每个partition的offset，这个offset是我们自己根据每个topic的不同partition生成的
    //数据路径例子：/consumers/${group}/offsets/${topic}/${partitionId}/${offset}"
    //zkTopicPath = /consumers/qingniu/offsets/hainiu_qingniu/
    val children = zkClient.countChildren(zkTopicPath)

    //判断zk中是否保存过历史的offset
    if (children > 0) {

      val zkOffsets: mutable.HashMap[TopicPartition, Long] = getEffectiveOffsets(zkClient,topic,topics,children,zkTopicPath)
      println("-------对比合并后 的 offset-------------")
      println(zkOffsets)

      //通过KafkaUtils创建直连的DStream，并使用fromOffsets中存储的历史偏离量来继续消费数据
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, zkOffsets))
    } else {
      //如果zk中没有该topic的历史offset，那就根据kafkaParam的配置使用最新(latest)或者最旧的(earliest)的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    //通过rdd转换得到偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    //创建5个自定义累加器, 用来记录每个批次中每个host的抽取情况
    val hostScanAccumulator = new MapAccumulator
    val hostFilteredAccumulator = new MapAccumulator
    val hostExtractAccumulator = new MapAccumulator
    val hostEmptyAccumulator = new MapAccumulator
    val hostNoMatchAccumulator = new MapAccumulator
    ssc.sparkContext.register(hostScanAccumulator)
    ssc.sparkContext.register(hostFilteredAccumulator)
    ssc.sparkContext.register(hostExtractAccumulator)
    ssc.sparkContext.register(hostEmptyAccumulator)
    ssc.sparkContext.register(hostNoMatchAccumulator)

    import org.elasticsearch.spark._

    // 定义广播变量                       host                   xpath                 人工机器xpath
    var broad: Broadcast[mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]] = ssc.sparkContext.broadcast(new mutable.HashMap[String ,mutable.HashMap[String, mutable.HashSet[String]]])

    // 更新时间间隔 10s, 每隔10秒钟跟新一次
    val updateInerval:Long = 10000

    // 广播变量最后一次更新时间
    var lastUpdateTime:Long = 0L


    //迭代DStream中的RDD，将每一个时间间隔对应的RDD拿出来，这个方法是在driver端执行
    //在foreachRDD方法中就跟开发spark-core是同样的流程了，当然也可以使用spark-sql
    kafkaStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //得到该RDD对应kafka消息的offset,该RDD是一个KafkaRDD，所以可以获得偏移量的范围
        //不使用transform可以直接在foreachRDD中得到这个RDD的偏移量，这种方法适用于DStream不经过任何的转换，
        //直接进行foreachRDD，因为如果transformation了那就不是KafkaRDD了，就不能强转成HasOffsetRanges了，从而就得不到kafka的偏移量了
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val dataRDD: RDD[String] = kafkaRDD.map(_.value())

        // 在初始化广播变量 和 更新间隔时间已经大于等于 指定的updateInerval
        if(broad.value.isEmpty || System.currentTimeMillis() - lastUpdateTime >= updateInerval){
          val configMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]] = loadXpathConfig

          println(configMap)

          // 卸载广播变量
          broad.unpersist()

          // 更新广播变量
          broad = ssc.sparkContext.broadcast(configMap)

          // 更新 广播变量最后的更新时间
          lastUpdateTime = System.currentTimeMillis()
          println(s"更新广播变量结束， ${lastUpdateTime}")
        }

        // 筛选
        val filterRdd: RDD[String] = dataRDD.filter(line => {
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
            // 反规则xpath数组
            val fxpaths = new ArrayBuffer[String]
            if (JavaUtil.isNotEmpty(xpathMap)) {
              val it: util.Iterator[util.Map.Entry[String, String]] = xpathMap.entrySet().iterator()
              while (it.hasNext) {
                val entry: util.Map.Entry[String, String] = it.next()
                val key = entry.getKey

                val value: String = entry.getValue
//                println(s"key:${key},value:${value}")

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
                println(s"content.length:${content.length}")
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

        // 把统计的累加器结果写入数据表，报表体现
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
                   |insert into ${Constants.MYSQL_REPORT_EXTRACT}
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

        // 更新offset到zookeeper
        for (o <- offsetRanges) {
          //  /consumers/qingniu/offsets/hainiu_qingniu/0
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //将该 partition 的 offset 保存到 zookeeper
          //  /consumers/qingniu/offsets/hainiu_qingniu/888
          println(s"${zkPath}__${o.untilOffset.toString}")
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  private def loadXpathConfig(): mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]] = {
    val configMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]()

    // 加载广播变量
    val fs: FileSystem = FileSystem.get(new Configuration())
    val files: Array[FileStatus] = fs.listStatus(new Path(Constants.CONFIG_PATH))
    for (file <- files) {
      val path: Path = file.getPath()
      breakable {
        if (path.toString.endsWith("_COPYING_")) {
          break()
        }
        // 用hdfs的读取方式写，不能用FileInputStream
        val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
        var line = ""
        line = reader.readLine()
        while (line != null) {
          val arr: Array[String] = line.split("\t")

          breakable(
            if (arr.length != 4) {
              break()
            } else {
              val host: String = arr(0)
              val xpath: String = arr(1)
              // 正反xpath
              val tfXpathType: String = arr(2)

              // host对应的xpathMap        正反xpath          人工机器xpath
              val xpathMap: mutable.HashMap[String, mutable.HashSet[String]] = configMap.getOrElseUpdate(host, new mutable.HashMap[String, mutable.HashSet[String]]())

              val set: mutable.HashSet[String] = xpathMap.getOrElseUpdate(tfXpathType, new mutable.HashSet[String])

              set.add(xpath)

            }
          )
          line = reader.readLine()

        }
        reader.close()

      }



    }
    configMap
  }

  /**
    *  获取有效的offset
    *
    *
    * @param zkClient
    * @param topic
    * @param topics
    * @param children
    * @param zkTopicPath
    * @return
    */
  def getEffectiveOffsets(zkClient:ZkClient, topic:String, topics:Set[String], children:Int, zkTopicPath:String):HashMap[TopicPartition, Long] = {
    //如果存在历史的偏移量，那使用zkOffsets来存放存储在zk中的每个TopicPartition对应的offset
    // 是外部存储zookeeper存的offset
    var zkOffsets = new HashMap[TopicPartition, Long]

    for (i <- 0 until children) {
      // /consumers/qingniu/offsets/hainiu_qingniu/0
      val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
      // hainiu_qingniu/0
      val tp = new TopicPartition(topic, i)
      //将每个partition对应的offset保存到fromOffsets中
      // hainiu_qingniu/0 -> 888
      zkOffsets += tp -> partitionOffset.toLong
    }
    println("----------zookeeper 维护的offset----------------")
    println(zkOffsets)

    //**********用于解决SparkStreaming程序长时间中断，再次消费时已记录的offset丢失导致程序启动报错问题
    import scala.collection.mutable.Map
    //存储kafka集群中每个partition当前最早的offset
    var clusterEarliestOffsets = Map[Long, Long]()
    val consumer: SimpleConsumer = new SimpleConsumer("s1.hadoop", 9092, 100000, 64 * 1024,
      "leaderLookup" + System.currentTimeMillis())
    //使用隐式转换进行java和scala的类型的互相转换
    import scala.collection.convert.wrapAll._
    val request: TopicMetadataRequest = new TopicMetadataRequest(topics.toList)
    val response: TopicMetadataResponse = consumer.send(request)
    consumer.close()

    val metadatas: mutable.Buffer[PartitionMetadata] = response.topicsMetadata.flatMap(f => f.partitionsMetadata)
    //从kafka集群中得到当前每个partition最早的offset值
    metadatas.map(f => {
      val partitionId: Int = f.partitionId
      val leaderHost: String = f.leader.host
      val leaderPort: Int = f.leader.port
      val clientName: String = "Client_" + topic + "_" + partitionId
      val consumer: SimpleConsumer = new SimpleConsumer(leaderHost, leaderPort, 100000,
        64 * 1024, clientName)

      val topicAndPartition = new TopicAndPartition(topic, partitionId)
      var requestInfo = new HashMap[TopicAndPartition, PartitionOffsetRequestInfo]();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime, 1));
      val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
      val response = consumer.getOffsetsBefore(request)
      val offsets: Array[Long] = response.offsets(topic, partitionId)
      consumer.close()
      clusterEarliestOffsets += ((partitionId, offsets(0)))
    }
    )
    println("-------kafka 数据 最早offset--------------")
    println(clusterEarliestOffsets)

    // 外循环是kafka offsets
    for ((clusterPartition, clusterEarliestOffset) <- clusterEarliestOffsets) {
      val tp = new TopicPartition(topic, clusterPartition.toInt)
      val option: Option[Long] = zkOffsets.get(tp)

      // kafka 有的分区，但zk 没有， 给zk新增分区
      if (option == None) {
        zkOffsets += (tp -> clusterEarliestOffset)
      } else {
        var zkOffset: Long = option.get
        if (zkOffset < clusterEarliestOffset) {
          zkOffset = clusterEarliestOffset
          zkOffsets += (tp -> zkOffset)
        }
      }
    }

    zkOffsets
  }
}
