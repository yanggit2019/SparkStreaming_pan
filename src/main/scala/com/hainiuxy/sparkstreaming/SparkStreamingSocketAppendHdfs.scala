package com.hainiuxy.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.SparkPartitionID
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
// 写入到hdfs，写入时会考虑到写小文件的情况
object SparkStreamingSocketAppendHdfs {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocketAppendHdfs")
    // 创建StreamingContext对象
    // 批次间隔5s,就是每5s产生一个批次，处理数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 创建socket流
    // socket流需要receiver接收并缓存，默认的缓存级别MEMORY_AND_DISK_SER_2，防止数据丢失
    //                                                                  ip        端口
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)

    val reduceDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    reduceDS.foreachRDD((rdd,t) =>{
      // 通过 coalesce 减少分区
      val rdd2: RDD[String] = rdd.coalesce(2).mapPartitionsWithIndex((partitionId, it) => {
        if (!it.isEmpty) {
          // 创建hdfs写入流
          val fs: FileSystem = FileSystem.get(new Configuration())

          val sdf = new SimpleDateFormat("yyyyMMddHH")
          val format: String = sdf.format(new Date())

          // 定义输出目录
//          val outputPath: String = s"/tmp/sparkstreaming/output_hdfs/${format}_${partitionId}"
          val outputPath: String = s"/user/panniu/spark/out_hdfs/${format}_${partitionId}"
          val outputDir = new Path(outputPath)

          // 如果文件存在就追加写入， 如果文件不存在就创建文件追加写入
          val fos: FSDataOutputStream = if (fs.exists(outputDir)) fs.append(outputDir) else fs.create(outputDir)

          // 一条一条写
          it.foreach(f => {
            fos.write(s"${f._1}\t${f._2}\n".getBytes("utf-8"))
          })

          // 关闭写入流
          fos.close()
        }

        List[String]().iterator
      })
      // 没有什么意义，单纯用于触发action
      rdd2.foreach(f => ())
    })

    // 开启流程序
    ssc.start()

    // 阻塞让流程一直跑下去，直到手动退出或异常退出
    ssc.awaitTermination()

  }
}
