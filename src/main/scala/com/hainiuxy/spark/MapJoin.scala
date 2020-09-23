package com.hainiuxy.spark

import com.hainiuxy.util.{OrcFormat, OrcUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

class MapJoin
object MapJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapjoin")
    val sc = new SparkContext(sparkConf)

    // orc输入目录
    val orcInputPath:String = "/tmp/spark/mapjoin_input"

    // 字典输入文件
    val dictFile:String = "/tmp/spark/country_dict.dat"

    // 读取字典文件内容
    val map: Map[String, String] = Source.fromFile(dictFile).getLines().toList.map(_.split("\t")).map(f => (f(0),f(1))).toMap
    
    // 用广播变量包装
    val broad: Broadcast[Map[String, String]] = sc.broadcast(map)
    
    // 定义匹配的累加器
    val hasCountryAcc: LongAccumulator = sc.longAccumulator
    // 定义没匹配的累加器
    val notHasCountryAcc: LongAccumulator = sc.longAccumulator
//    path: String,     输入目录
//    fClass: Class[F], inputformatclass
//    kClass: Class[K], keyinclass
//    vClass: Class[V], valueinclass
    // 读取orc文件构建Rdd
    val orcRdd: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(
      orcInputPath,
      classOf[OrcNewInputFormat],
      classOf[NullWritable],
      classOf[OrcStruct])
    val orcWriteRdd: RDD[(NullWritable, Writable)] = orcRdd.mapPartitions(it => {
      // 创建orcUtil对象，设置读的schema
      val orcUtil = new OrcUtil
      orcUtil.setOrcTypeReadSchema(OrcFormat.SCHEMA)
      orcUtil.setOrcTypeWriteSchema("struct<code:string,name:string>")

      val countryMap: Map[String, String] = broad.value

      // 定义能装orc序列化数据的列表
      val orcWriteList = new ListBuffer[(NullWritable, Writable)]()

      // 迭代分区内的每个行数据
      it.foreach(f => {
        val countryCode: String = orcUtil.getOrcData(f._2, "country")
        val option: Option[String] = countryMap.get(countryCode)

        if (option == None) {
          // 没匹配上
          notHasCountryAcc.add(1L)
        } else {
          // 匹配上
          hasCountryAcc.add(1L)
          val countryName: String = option.get

          // 把匹配的结果写入orc
          orcUtil.addAttr(countryCode, countryName)

          val w: Writable = orcUtil.serialize()
          orcWriteList += ((NullWritable.get(), w))

        }

      })

      orcWriteList.iterator
    })

    val orcOutputPath:String = "/tmp/spark/mapjoin_output"

    import com.hainiuxy.util.MyPredef.string2HdfsUtil
    orcOutputPath.deleteHdfs

    val hadoopConf = new Configuration()
    // 设置输出orc文件采用snappy压缩
    hadoopConf.set("orc.compress", classOf[SnappyCodec].getName)
//    path: String,          输出目录
//    keyClass: Class[_],    keyOutclass
//    valueClass: Class[_],  valueoutclass
//    outputFormatClass:     outputFormatClass
//    conf: Configuration = self.context.hadoopConfiguration)
    // 写入orc文件
    orcWriteRdd.saveAsNewAPIHadoopFile(orcOutputPath, classOf[NullWritable],classOf[Writable], classOf[OrcNewOutputFormat])
    println(s"notmatch:${notHasCountryAcc.value}")
    println(s"match:${hasCountryAcc.value}")
  }
}
