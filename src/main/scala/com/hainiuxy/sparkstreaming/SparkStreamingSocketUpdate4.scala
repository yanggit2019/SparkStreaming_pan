package com.hainiuxy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketUpdate4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSocketUpdate4")

    val checkPointPath = "/tmp/sparkstreaming/socket_update_state_checkpoint4"

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
        val reduceByKeyDS: DStream[(String, StateValue)] = socketDS.flatMap(_.split(" "))
          .map((_, StateValue(1)))
          .reduceByKey((v1,v2) =>{
            StateValue(v1.num + v2.num)
        })

        val updateStateDS: DStream[(String, StateValue)] = reduceByKeyDS.updateStateByKey((nowList: Seq[StateValue], lastOption: Option[StateValue]) => {
          // 当前批次的汇总
          var sum: Int = 0
          for (v <- nowList) {
            sum += v.num
          }
          // 获取key对应的上一批次的value值
          val lastValue: StateValue = if (lastOption.isDefined) lastOption.get else StateValue(0)


          // 如果本批次有，那就需要更新
          if(sum > 0){
            Some(StateValue(sum + lastValue.num, true))
          }else{
            // 如果本批次没有，就不需要更新
            Some(StateValue(sum + lastValue.num, false))
          }

        })


        updateStateDS.foreachRDD((rdd, t) => {
          // 只筛选本批次有的数据，写入数据库
          println(s"time:${t}, 要写入数据库的data:${rdd.filter(_._2.isUpdate).collect.toBuffer}")

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

// 定义样例类，带有状态的数值，默认状态是不更新
case class StateValue(val num:Int, val isUpdate:Boolean = false)
