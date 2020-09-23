package com.hainiuxy.offset

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 手动更新偏移量
  * 将偏移量提交到kafka
  */
object SparkStreamingKafkaOffsetNotAutoCommit {
  def main(args: Array[String]): Unit = {

    val group = "group65"
    val topic = "hainiu_sk3"
    val conf = new SparkConf().setAppName("sparkstreamingkafkaoffset").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //可以通过streamingContext中的sparkContext来设置日志级别或者其他参数
    //    streamingContext.sparkContext.setLogLevel("info")

    //kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "earliest",
      //修改为手动提交偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    //这种方式是在Kafka中记录读取偏移量
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )

    //迭代DStream中的RDD，将每一个时间间隔对应的RDD拿出来，这个方法是在driver端执行
    //在foreachRDD方法中就跟开发spark-core是同样的流程了，当然也可以使用spark-sql
    stream.foreachRDD { rdd =>
      //获取该RDD对应的偏移量，记住只有kafka的rdd才能强转成HasOffsetRanges类型
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //执行这个rdd的aciton，这里rdd的算子是在集群上执行的
      rdd.foreach { line =>
        println(line.key() + " " + line.value())
      }
      //foreach和foreachPartition的区别
      //foreachPartition不管有没有数据都会执行自己的function
      //foreach只在有数据时执行自己的function
      //      rdd.foreachPartition(it =>{
      //        val list: List[ConsumerRecord[String, String]] = it.toList
      //        println(list)
      //      })

      for (o <- offsetRanges) {
        val zkPath = s"partitioner:${o.partition}_offset:${o.untilOffset.toString}||"
        print(zkPath)
      }

      println()
      //更新偏移量
      //在上面所有的流程走完之后，再更新偏移量，为什么放到后面？因为如果上面抛异常了这里就不用更新了
      //这里如果不提交，那在下次启动的时候DirectStream将在原来的位置重新处理
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
