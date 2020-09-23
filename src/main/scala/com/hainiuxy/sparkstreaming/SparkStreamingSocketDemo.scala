package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocket1")

//    conf.set("spark.default.parallelism", "5")
    // 创建StreamingContext对象
    val ssc = new StreamingContext(conf, Durations.seconds(1))

    // 创建socket流
    // socket流需要receiver接收并缓存，默认的缓存级别MEMORY_AND_DISK_SER_2，防止数据丢失
    //                                                                  ip        端口
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)

    // DStream ---> rdd ---> rdd --->result
    socketDS.foreachRDD((rdd,t) =>{
      val reduceByKeyRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//      Thread.sleep(2000)
      println(s"time:${t}, data:${reduceByKeyRdd.collect().toBuffer}")
    })

    // 开启流程序
    ssc.start()

    // 阻塞让流程一直跑下去，直到手动退出或异常退出
    ssc.awaitTermination()

  }
}
