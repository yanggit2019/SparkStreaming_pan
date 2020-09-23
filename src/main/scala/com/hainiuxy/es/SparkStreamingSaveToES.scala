package com.hainiuxy.es

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSaveToES {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSaveToES")
    // 设置es的节点
    conf.set("es.nodes", "s1.hadoop")
    // 设置es通信端口
    conf.set("es.port", "9200")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    // 引入隐式转换
    import org.elasticsearch.spark._

    // 读取socket流   java liu --> course->java,tname->liu
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)

    socketDS.foreachRDD((rdd, t) =>{

      val mapRdd: RDD[Map[String, String]] = rdd.map(f => {
        val arr: Array[String] = f.split(" ")
        Map[String, String]("course" -> arr(0), "tname" -> arr(1))
      })

      // 通过隐式转换，给RDD赋予了能写入es的能力
      // 给 RDD[T] 赋予 SparkRDDFunctions[T]的能力
      // 是个action算子
      mapRdd.saveToEs("course_index/course_type")

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
