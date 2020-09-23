package com.hainiuxy.kafka

import java.util.Properties

import com.hainiuxy.kafka.jk.{HainiuKafkaDeserializer, HainiuKafkaSerializer, MyPartitioner}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.actors.Actor

// 自定义kafka消息类型
//case class KafkaData(val msg:String)

// 定义Producer线程
class HainiuKafkaProducerSer2 extends Actor{

  var topic:String = _

  var kafkaProducer: KafkaProducer[String, KafkaData] = _
  // 定义辅助构造器
  def this(topic:String) = {
    this()
    this.topic = topic
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer", classOf[StringSerializer].getName())
    // 设置要发送的消息的序列化类型
    props.put("value.serializer", classOf[HainiuKafkaSerializer].getName())


    this.kafkaProducer = new KafkaProducer(props)
  }


  override def act(): Unit = {
    Thread.sleep(6000)
    var num:Int = 1
    while (true) {
      val messageStr = new String("hainiu_" + num)
      System.out.println("send:" + messageStr)
      // 发送到kafka
      this.kafkaProducer.send(new ProducerRecord[String,KafkaData](this.topic, KafkaData(messageStr)))
      num += 1
      if(num > 10){
        num = 0
      }
      Thread.sleep(3000)
    }

  }
}

// 定义Consumer线程
class HainiuKafkaConsumerDeser2 extends Actor{

  var topic:String = _

  var kafkaConsumer:KafkaConsumer[String, KafkaData] = _

  def this(topic:String) = {
    this()
    this.topic = topic
    val pro = new Properties()

    // 更高级的api，直接读broker
    pro.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    //每个consumer在消费数据的时候指定自己属于那个consumerGroup
    pro.put("group.id", "group17")
    //consumer读取的策略
//    pro.put("auto.offset.reset", "earliest")
    pro.put("auto.offset.reset", "latest")
    //是否自动提交offset
    pro.put("enable.auto.commit", "true")
    //多长时间提交一次
    pro.put("auto.commit.interval.ms", "1000")
    //使用String的序列化工具把二进制转成String类型
    pro.put("key.deserializer", classOf[StringDeserializer].getName)
    //使用自定义的序列化工具把二进制转成StreamingStateValue
    pro.put("value.deserializer", classOf[HainiuKafkaDeserializer].getName)

    this.kafkaConsumer = new KafkaConsumer[String, KafkaData](pro)
    //指定consumer读取topic中的那一个partition，这个叫做分配方式
    //    this.consumer.assign(java.util.Arrays.asList(new TopicPartition(topic,2)))

    //指定consumer读取topic中所有的partition，这个叫做订阅方式
    this.kafkaConsumer.subscribe(java.util.Arrays.asList(topic))

  }


  override def act(): Unit = {
    while(true){
      val records: ConsumerRecords[String, KafkaData] = this.kafkaConsumer.poll(100)
      // 需要通过隐式转换把java的 Iterable 转成 scala 的 Iterable
      import scala.collection.convert.wrapAll._
      for(record <- records){
        val msg: KafkaData = record.value()
        val topicName: String = record.topic()
        val partitionId: Int = record.partition()
        val offset: Long = record.offset()

        println(s"${topicName}\t${msg}\t${partitionId}\t${offset}")

      }
    }

  }
}


object HainiuKafkaSerAndDeser{
  def main(args: Array[String]): Unit = {
    val topic:String = "hainiu_obj2"
    val producer = new HainiuKafkaProducerSer2(topic)
    val consumer = new HainiuKafkaConsumerDeser2(topic)
    consumer.start()
    producer.start()
  }
}
