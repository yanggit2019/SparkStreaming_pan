package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount3")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("E:\\tmp\\spark\\input")

    println(s"partition num:${rdd.getNumPartitions}")

    // 默认的 partitioner 采用 HashPartitioner
    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_,1)).reduceByKey(_ + _)
    // partitioner 采用 RangePartitioner，保证分区间有序，分区内无序
    // 再让分区内有序就可以实现全局有序
    val resRdd2: RDD[(String, Int)] = resRdd.sortBy(_._2,false)

    val outputPath = "/tmp/spark/output_wc"
    import com.hainiuxy.util.MyPredef.string2HdfsUtil
    outputPath.deleteHdfs

    resRdd2.saveAsTextFile(outputPath)

    // 打印rdd的阶段划分
    println(resRdd2.toDebugString)

  }
}
