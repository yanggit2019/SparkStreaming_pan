package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketUpdate3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSocketUpdate3")

    val checkPointPath = "/tmp/sparkstreaming/socket_update_state_checkpoint3"

    // alt + shift + m  将代码抽成方法
    val createStreamingContext: () => StreamingContext = {
      () => {
        // 创建StreamingContext对象
        // 批次间隔5s,就是每5s产生一个批次，处理数据
        val sc = new StreamingContext(conf, Durations.seconds(5))
        // 设置checkpoint
        sc.checkpoint(checkPointPath)

        val socketDS: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 6666)
        // 当前批次的汇总
        val reduceByKeyDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        val updateStateDS: DStream[(String, Int)] = reduceByKeyDS.updateStateByKey((nowList: Seq[Int], lastOption: Option[Int]) => {
          // 当前批次的汇总
          var sum: Int = 0
          for (v <- nowList) {
            sum += v
          }
          // 获取key对应的上一批次的value值
          val lastValue: Int = if (lastOption.isDefined) lastOption.get else 0

          Some(sum + lastValue)
        })

        updateStateDS.foreachRDD((rdd, t) => {
          println(s"time:${t}, data:${rdd.collect.toBuffer}")
        })
        sc
      }
    }

    // getOrCreate 的作用是 当checkPointPath没有数据时，执行函数构建StreamingContext对象；
    // 当checkPointPath有数据时，直接用checkpoint里的StreamingContext对象。
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPointPath, createStreamingContext)

    ssc.start()
    ssc.awaitTermination()

  }
}
