package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
// 两个socket流union
object SparkStreamingSocketUnion9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSocketUnion9")
    // 创建StreamingContext对象
    // 批次间隔10s,就是每10s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(10))

    // 构建socket1
    val socket1DS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)
    // 构建socket2
    val socket2DS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 7777)

    // 直接把 receiver接收过来的流union
//    val unionDS: DStream[String] = socket1DS.union(socket2DS)
//    val reduceDS: DStream[(String, Int)] = unionDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    // 有的时候得需要加工下在union
    val word1DS: DStream[String] = socket1DS.flatMap(_.split("&"))
    val word2DS: DStream[String] = socket2DS.flatMap(_.split(" "))
    val unionDS: DStream[String] = word1DS.union(word2DS)
    val reduceDS: DStream[(String, Long)] = unionDS.countByValue()

    reduceDS.foreachRDD((rdd,t) =>{
      println(s"time:${t}, data:${rdd.collect().toBuffer}")
    })
    // 开启流程序
    ssc.start()

    // 阻塞让流程一直跑下去，直到手动退出或异常退出
    ssc.awaitTermination()

  }
}
