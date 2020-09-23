//package com.hainiu.spark.streaming.offset
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//object SparkStreamingKafkaStream {
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("sparkstreamingkafkastream").setMaster("local[*]")
//
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val zkQuorum = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"
//    val groupId = "group15"
//    val topic = Map[String, Int]("hainiu" -> 1)
//
//    //创建DStream，需要KafkaDStream
//    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
//    //对数据进行处理
//    val lines: DStream[String] = data.map(_._2)
//    //切分压平
//    val words: DStream[String] = lines.flatMap(_.split(" "))
//    //单词和一组合在一起
//    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
//    //聚合
//    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
//    //打印结果这是一个Action
//    reduced.print()
//    //启动sparksteaming程序
//    ssc.start()
//    //等待退出
//    ssc.awaitTermination()
//
//  }
//
//}
