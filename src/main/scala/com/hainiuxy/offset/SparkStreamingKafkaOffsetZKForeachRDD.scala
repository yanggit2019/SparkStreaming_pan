package com.hainiuxy.offset

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.HashMap

/**
  * 偏移量保存到zk中
  * 不使用DStream的transform等其它算子
  * 将DStream数据处理方式转成纯正的spark-core的数据处理方式
  */
object SparkStreamingKafkaOffsetZKForeachRDD {
  def main(args: Array[String]): Unit = {
    //指定组名
    val group = "lishuai11"
    //创建SparkConf
    val conf = new SparkConf().setAppName("SparkStreamingKafkaOffsetZK").setMaster("local[*]")
    //创建SparkStreaming，设置间隔时间
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    //指定 topic 名字
    val topic = "hainiu_html"
    //指定kafka的broker地址，SparkStream的Task直连到kafka的分区上，用底层的API消费，效率更高
    val brokerList = "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"
//    val brokerList = "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    //指定zk的地址，更新消费的偏移量时使用，当然也可以使用Redis和MySQL来记录偏移量
    val zkQuorum = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"
    //SparkStreaming时使用的topic集合，可同时消费多个topic
    val topics: Set[String] = Set(topic)
    //topic在zk里的数据路径，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //得到zk中的数据路径 例如："/consumers/${group}/offsets/${topic}"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "earliest"
    )

    //定义一个空的kafkaStream，之后根据是否有历史的偏移量进行选择
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null

    //如果存在历史的偏移量，那使用fromOffsets来存放存储在zk中的每个TopicPartition对应的offset
    var fromOffsets = new HashMap[TopicPartition, Long]

    //创建zk客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)

    //从zk中查询该数据路径下是否有每个partition的offset，这个offset是我们自己根据每个topic的不同partition生成的
    //数据路径例子：/consumers/${group}/offsets/${topic}/${partitionId}/${offset}"
    //zkTopicPath = /consumers/group1311/offsets/hainiu_test/
    val children = zkClient.countChildren(zkTopicPath)

    //判断zk中是否保存过历史的offset
    if (children > 0) {
      for (i <- 0 until children) {
        // /consumers/qingniu/offsets/hainiu_qingniu/0
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // hainiu_qingniu/0
        val tp = new TopicPartition(topic, i)
        //将每个partition对应的offset保存到fromOffsets中
        // hainiu_qingniu/0 -> 888
        fromOffsets += tp -> partitionOffset.toLong
      }
//      println(fromOffsets)
      //通过KafkaUtils创建直连的DStream，并使用fromOffsets中存储的历史偏离量来继续消费数据
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    } else {
      //如果zk中没有该topic的历史offset，那就根据kafkaParam的配置使用最新(latest)或者最旧的(earliest)的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    //通过rdd转换得到偏移量的范围
    var offsetRanges = Array[OffsetRange]()


    //迭代DStream中的RDD，将每一个时间间隔对应的RDD拿出来，这个方法是在driver端执行
    //在foreachRDD方法中就跟开发spark-core是同样的流程了，当然也可以使用spark-sql
    kafkaStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //得到该RDD对应kafka消息的offset,该RDD是一个KafkaRDD，所以可以获得偏移量的范围
        //不使用transform可以直接在foreachRDD中得到这个RDD的偏移量，这种方法适用于DStream不经过任何的转换，
        //直接进行foreachRDD，因为如果transformation了那就不是KafkaRDD了，就不能强转成HasOffsetRanges了，从而就得不到kafka的偏移量了
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val dataRDD: RDD[String] = kafkaRDD.map(_.value())

        //执行这个rdd的aciton，这里rdd的算子是在集群上执行的
        dataRDD.foreachPartition(partition =>
          partition.foreach(x => {
            println(x)
          })
        )

        // 将最新的offset更新到zookeeper外部存储
        for (o <- offsetRanges) {
          //  /consumers/qingniu/offsets/hainiu_qingniu/0
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //将该 partition 的 offset 保存到 zookeeper
          //  /consumers/qingniu/offsets/hainiu_qingniu/888
          println(s"${zkPath}__${o.untilOffset.toString}")
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
