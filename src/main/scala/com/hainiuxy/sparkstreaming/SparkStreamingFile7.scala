package com.hainiuxy.sparkstreaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
// 读文件流
object SparkStreamingFile7 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkStreamingFile7")
    // 设置文件流的最小记录时间, 一个月内的
    conf.set("spark.streaming.fileStream.minRememberDuration","2592000s")
    // 创建StreamingContext对象
    // 批次间隔5s,就是每5s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))


    val inputPath:String = "E:\\tmp\\sparkstreaming\\input_file"

    // 如果 newFilesOnly为false，那目录下的文件的创建时间要在一个月之内，一个月之外的它检测不到；
    // 如果为true，文件需要在流式程序启动以后创建
    val fileDS: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath,
      (path: Path) => path.toString.endsWith(".txt"),
      true)
//    val hadoopConf = new Configuration()
    // 设置文件流的最小记录时间
//    hadoopConf.set("spark.streaming.fileStream.minRememberDuration","2592000s")
//    val fileDS: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath,
//      (path: Path) => path.toString.endsWith(".txt"),
//      false, hadoopConf)

    val reduceDS: DStream[(String, Int)] = fileDS.flatMap(_._2.toString.split(" ")).map((_,1)).reduceByKey(_ + _)
    reduceDS.foreachRDD((rdd,t) =>{
      println("aaa")
//      println(s"time:${t}, data:${rdd.collect.toBuffer}")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
