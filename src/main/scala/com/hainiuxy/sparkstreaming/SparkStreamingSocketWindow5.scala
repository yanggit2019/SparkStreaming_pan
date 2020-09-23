package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreamingSocketWindow5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocketWindow5")
    // 创建StreamingContext对象
    // 批次间隔5s,就是每5s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 创建socket流
    // socket流需要receiver接收并缓存，默认的缓存级别MEMORY_AND_DISK_SER_2，防止数据丢失
    //                                                                  ip        端口
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)

    val reduceDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
//    // 需要攒4个批次的处理结果，所以需要缓存数据
//    // 默认的缓存级别StorageLevel.MEMORY_ONLY_SER，不需要程序员设置
    val windowDS: DStream[(String, Int)] = reduceDS.window(Durations.seconds(20), Durations.seconds(10))
    windowDS.foreachRDD((rdd,t) =>{
      // 每隔滑动间隔触发一次
      println(s"time:${t}, data:${rdd.collect().toBuffer}")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
