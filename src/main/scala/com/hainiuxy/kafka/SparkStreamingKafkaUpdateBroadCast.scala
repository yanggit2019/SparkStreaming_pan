package com.hainiuxy.kafka

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.util.LongAccumulator

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkStreamingKafkaUpdateBroadCast {
  def main(args: Array[String]): Unit = {
    // 创建kafka直连流，没有receiver接收，所以可以用local[1]
    // 注意：分配的CPU核数最好要大于等于kafka的分区数，这样可以每个cpu核对应一个分区
    //这里设置cpu cores为1的时候也能运行，说明KafkaUtils.createDirectStream是不需要receiver占用一个cpu cores的
    //而KafkaUtils.createStream是需要的，这种模式是出现在sparkStreaming-kafka-0.80版本的，0.10.x版本已经抛弃了
    //这个的cpu为什么设置为5，因为对于流式计算来说，应该快速完成每个任务的运算，所以在任务生成的时候每个task都应有对应的cpu，不让其等待，这样运算速度才会更快
    //再者由于是本地环境，所以这个5个cpu中有一个cpu是为了driver设置的，在实现的分布式环境中设置executor为5，就是kafka中topic的partition数据就可以
    val sparkConf: SparkConf = new SparkConf().setMaster("local[5]").setAppName("SparkStreamingKafkaUpdateBroadCast")

    //设置blockInterval来调整并发数，也就是spark的RDD分区数，也就是task的数量
    //这个配置对于StreamingContext创建带receiver的流是起作用的，比如socket或者KafkaUtils.createStream
    //对于KafkaUtils.createDirectStream（kafka的直连模式）创建的流是不起作用的
    //因为直连模式是根据topic的分区数来决定并发度的，也就是task会直接连接到kafka中的topic的partition上
    //所以这里设置blockInterval对RDD的partition的划分是不起作用的
    //conf.set("spark.streaming.blockInterval","1000ms")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))

    val topics = "hainiu_sk2"

    // 消费kafka的配置
    // sparkStreaming 直连kafka的分区，kafka有多少分区，sparkStreaming流就有多少分区
    // 直连kafka，消费kafka时直接读broker，消费的offset也维护到broker（topic：__consumer_offsets）
    val kafkaParams = new mutable.HashMap[String,Object]()
    kafkaParams += "bootstrap.servers" -> "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    kafkaParams += "group.id" -> "group20"
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "auto.offset.reset" -> "earliest"
    kafkaParams += "enable.auto.commit" -> "true"
    // 设置位置策略为 executor 与 kafka分区均分
    val location: LocationStrategy = LocationStrategies.PreferConsistent

    // 用于装要union在一起的流
    val kafkaStreams = new ListBuffer[InputDStream[ConsumerRecord[String, String]]]

    /// 创建了两个流
    for(i <- 0 until 2){
      val partitions = new ListBuffer[TopicPartition]
      for(j <- (i * 2) until (i * 2) + 2){
        val partition = new TopicPartition(topics, j)
        partitions += partition
      }

      // 设置消费策略为分配方式
      val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Assign(partitions, kafkaParams)

      // 创建kafka直连流
      val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, location, consumerStrategy)
      kafkaStreams += kafkaDS

    }

    // 将多个kafka流union在一起
    val unionDS: DStream[ConsumerRecord[String, String]] = ssc.union(kafkaStreams)

    val reduceDS: DStream[(String, Int)] = unionDS.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_ + _)

    val countryMap = new mutable.HashMap[String, String]


    // 定义两个累加器
    val matchAcc: LongAccumulator = ssc.sparkContext.longAccumulator
    val notMatchAcc: LongAccumulator = ssc.sparkContext.longAccumulator

    // 定义广播变量
    var broadcast: Broadcast[mutable.HashMap[String, String]] = ssc.sparkContext.broadcast(countryMap)

    // 更新时间间隔 10s, 每隔10秒钟跟新一次
    val updateInerval:Long = 10000

    // 广播变量最后一次更新时间
    var lastUpdateTime:Long = 0L

    reduceDS.foreachRDD((rdd, t) =>{

      // 在driver端运行
      // 在初始化广播变量 和 更新间隔时间已经大于等于 指定的updateInerval
      if(broadcast.value.isEmpty || System.currentTimeMillis() - lastUpdateTime >= updateInerval){
        println("更新广播变量开始")
        // 读取配置目录的数据更新到广播变量
        val configPath:String = "/tmp/sparkstreaming/input_updateSparkBroadCast"
        val fs: FileSystem = FileSystem.get(new Configuration())
        val files: Array[FileStatus] = fs.listStatus(new Path(configPath))
        for(fileStatus <- files){
          val path: Path = fileStatus.getPath()
          // 用hdfs的读取方式写，不能用FileInputStream
          val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
          var line:String = ""
          line = reader.readLine()
          while(line != null){
            val arr: Array[String] = line.split(" ")

            breakable(
              if(arr.length != 2){
                break()
              }else{
                val code: String = arr(0)
                val name: String = arr(1)
                countryMap += (code -> name)
              }

            )
            line = reader.readLine()
          }
          reader.close()
        }

        // 卸载广播变量
        broadcast.unpersist()

        // 更新广播变量
        broadcast = ssc.sparkContext.broadcast(countryMap)

        // 更新 广播变量最后的更新时间
        lastUpdateTime = System.currentTimeMillis()
        println(s"更新广播变量结束， ${lastUpdateTime}")
      }

      // 下面的处理数据
      // 判断流式处理中，rdd是否为null
      if(!rdd.isEmpty()){
        rdd.foreachPartition(it =>{
          val configMap: mutable.HashMap[String, String] = broadcast.value

          it.foreach(f =>{
            val countryCode: String = f._1
            val num: Int = f._2
            // 没匹配上
            if(configMap.get(countryCode) == None){
              notMatchAcc.add(1)
            }else{
              // 匹配上
              matchAcc.add(1)
              val countryName: String = configMap.get(countryCode).get

              // 可以把匹配上的写入数据库
              println(s"${countryCode}\t${countryName}\t${num}")

            }
          })

        })
      }

      // 输出本批次累加器的结果
      println(s"matchAcc:${matchAcc.value}")
      println(s"notMatchAcc:${notMatchAcc.value}")

      // 清空本批次累加器结果
      matchAcc.reset()
      notMatchAcc.reset()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
