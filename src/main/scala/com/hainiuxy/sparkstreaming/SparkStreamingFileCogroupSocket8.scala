package com.hainiuxy.sparkstreaming

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

// 文件流与socket流cogroup， 也就是join
object SparkStreamingFileCogroupSocket8 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingFileCogroupSocket8")
    // 设置文件流的最小记录时间, 一个月内的
    conf.set("spark.streaming.fileStream.minRememberDuration","2592000s")
    // 创建StreamingContext对象
    // 批次间隔5s,就是每5s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))


    val inputPath:String = "E:\\tmp\\sparkstreaming\\input_file2"


    // 文件流
    // 如果 newFilesOnly为false，那目录下的文件的创建时间要在一个月之内，一个月之外的它检测不到；
    // 如果为true，文件需要在流式程序启动以后创建
    val fileDS: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath,
      (path: Path) => path.toString.endsWith(".txt"),
      false)
    //                    国家码  国家名称
    val fileDS2: DStream[(String, String)] = fileDS.map(_._2.toString.split(" ")).map(f => (f(0), f(1)))

    // socket流
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)
    val socketDS2: DStream[(String, Long)] = socketDS.flatMap(_.split(" ")).countByValue()
    //                     国家码     国家名称集合      数量集合
//    val cogroupDS: DStream[(String, (Iterable[String], Iterable[Long]))] = fileDS2.cogroup(socketDS2)
//    cogroupDS.foreachRDD((rdd, t) =>{
//      rdd.foreach(f =>{
//        val code: String = f._1
//        val name: Iterable[String] = f._2._1
//        val num: Iterable[Long] = f._2._2
//        println(s"countryCode:${code}, countryName:${name}, countryCount:${num}")
//      })
//    })

    val joinDS: DStream[(String, (String, Long))] = fileDS2.join(socketDS2)
    joinDS.foreachRDD((rdd, t) =>{
      rdd.foreach(f =>{
        val code: String = f._1
        val name: String = f._2._1
        val num: Long = f._2._2
        println(s"countryCode:${code}, countryName:${name}, countryCount:${num}")
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
