package com.hainiuxy.kafka

import java.util.Properties

import kafka.consumer
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.actors.Actor
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// 定义Producer线程
class HainiuKafkaProducer extends Actor{

  var topic:String = _

  var kafkaProducer: KafkaProducer[String, String] = _
  // 定义辅助构造器
  def this(topic:String) = {
    this()
    this.topic = topic
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer", classOf[StringSerializer].getName())
    props.put("value.serializer", classOf[StringSerializer].getName())
    this.kafkaProducer = new KafkaProducer(props)
  }


  override def act(): Unit = {
    Thread.sleep(6000)
    var num:Int = 1
    while (true) {
      val messageStr = new String("hainiu_" + num)
      System.out.println("send:" + messageStr)
      // 发送到kafka
      this.kafkaProducer.send(new ProducerRecord[String,String](this.topic, messageStr))
      num += 1
      if(num > 10){
        num = 0
      }
      Thread.sleep(300)
    }

  }
}

// 定义Consumer线程
class HainiuKafkaConsumer extends Actor{

  var topic:String = _

  var consumerConnector: consumer.ConsumerConnector = _


  def this(topic:String) = {
    this()
    this.topic = topic
    val props = new Properties()
    props.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    // 消费者组id，一个组下可以有多个消费者实例
    props.put("group.id", "group14")
    props.put("zookeeper.session.timeout.ms", "60000")
    // 创建连接参数Properties，创建consumer上下文对象ConsumerConfig，
    // 通过Consumer类的create方法创建connector接口；
    this.consumerConnector = Consumer.create(new  ConsumerConfig(props))

  }


  override def act(): Unit = {
    // topicCountMap中包含topicName 和 streamNum
    val topicCountMap = new mutable.HashMap[String,Int]()
    // 设置一个线程来读取对应topic 分区的数据
    // 一个组里面有一个消费实例，那这个实例会读取对应topic的所有分区数据
    topicCountMap += (this.topic -> 1)

    // 通过connector的createMessageStreams方法创建信息流
    val createMessageStreams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumerConnector.createMessageStreams(topicCountMap)

    // 获取一个数据流
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = createMessageStreams.get(topic).get(0)
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
    while(it.hasNext()){
      val messageAndMetadata: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
      val listBuffer = new ListBuffer[Any]
      // 获取数据
      val bytes: Array[Byte] = messageAndMetadata.message()
      // 获取topic
      val topicName: String = messageAndMetadata.topic

      // 获取分区
      val partition: Int = messageAndMetadata.partition
      // 获取分区的offset偏移量
      val offset: Long = messageAndMetadata.offset

      println(s"${topicName}\t${new String(bytes)}\t${partition}\t${offset}")

      Thread.sleep(1000)
    }
  }


//  def act(): Unit = {
//    //两个consumer线程 消费 kafka topic 的2个分区的数据
//    var topicCountMap = new mutable.HashMap[String,Int]()
//    //用两个消费线程消费 topic 所有分区的数据
//    topicCountMap += ((topic, 2))
//    val createMessageStreams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = this.consumerConnector.createMessageStreams(topicCountMap)
//    val streams: List[KafkaStream[Array[Byte], Array[Byte]]] = createMessageStreams.get(topic).get
//    for(stream <- streams){
//      //每个KafkaStream 启动个消费线程消费
//      new Thread(new Runnable {
//        def run(): Unit = {
//          val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
//
//          while(it.hasNext()){
//            val next: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
//            val list:ListBuffer[Any] = ListBuffer[Any]()
//            val msg: Array[Byte] = next.message()
//            val topicName: String = next.topic
//            val partitionId: Int = next.partition
//            val offset: Long = next.offset
//            list.append(Thread.currentThread().getName)
//            list.append(new String(msg))
//            list.append(topicName)
//            list.append(partitionId)
//            list.append(offset)
//
//            println(s"receive: ${list}")
//            Thread.sleep(1)
//          }
//        }
//      }).start()
//
//    }
//  }
}


object HainiuKafkaDemo{
  def main(args: Array[String]): Unit = {
    val topic:String = "hainiu_sk"
    val producer = new HainiuKafkaProducer(topic)
//    val consumer = new HainiuKafkaConsumer(topic)
//    consumer.start()
    producer.start()
  }
}
