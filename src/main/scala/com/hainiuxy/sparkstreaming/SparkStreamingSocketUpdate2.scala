package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketUpdate2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSocketUpdate2")

    val checkPointPath = "/tmp/sparkstreaming/socket_update_state_checkpoint"

    // 创建StreamingContext对象
    // 批次间隔5s,就是每5s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 设置checkpoint
    ssc.checkpoint(checkPointPath)

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)
    // 当前批次的汇总
    val reduceByKeyDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

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

    updateStateDS.foreachRDD((rdd, t) =>{
      println(s"time:${t}, data:${rdd.collect.toBuffer}")
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
