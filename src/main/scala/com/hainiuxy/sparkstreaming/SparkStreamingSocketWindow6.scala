package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
// 带有checkpoint的window函数
object SparkStreamingSocketWindow6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocketWindow6")


    val checkpointPath = "/tmp/sparkstreaming/socket_window_check2"

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath,
      () => {
        // 创建StreamingContext对象
        // 批次间隔5s,就是每5s产生一个批次，处理数据
        val sc = new StreamingContext(conf, Durations.seconds(5))

        sc.checkpoint(checkpointPath)

        // 创建socket流
        // socket流需要receiver接收并缓存，默认的缓存级别MEMORY_AND_DISK_SER_2，防止数据丢失
        //                                                                  ip        端口
        val socketDS: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 6666)

        // 由于 countByValueAndWindow 调用了带有逆向reduce的reduceByKeyAndWindow函数
        // 所以需要有checkpoint
//        val windowDS: DStream[(String, Long)] = socketDS.flatMap(_.split(" "))
//          .countByValueAndWindow(Durations.seconds(20), Durations.seconds(10))

        // 这个和上面的逻辑一样
        val windowDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1))
          .reduceByKeyAndWindow(_ + _, _ - _, Durations.seconds(20), Durations.seconds(10))

        windowDS.foreachRDD((rdd, t) => {
          // 每隔滑动间隔触发一次
          println(s"time:${t}, data:${rdd.collect().toBuffer}")
        })
        sc
      })
    ssc




    ssc.start()
    ssc.awaitTermination()
  }
}
