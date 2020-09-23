package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("PartitionDemo")
    val sc = new SparkContext(conf)


//    val rdd: RDD[Int] = sc.parallelize(List(1,3,2,4,5,6),2)
//
//    println(rdd.getNumPartitions)
//    // 少分区变多分区用repartition
//    val rdd2: RDD[Int] = rdd.repartition(4)
//    println(s"repartition后分区数：${rdd2.getNumPartitions}")
//    // 多分区变少分区用 coalesce
//    val rdd3: RDD[Int] = rdd2.coalesce(2)
//    println(s"coalesce后分区数：${rdd3.getNumPartitions}")
//
//    rdd3.count()
    val rdd = sc.parallelize(List("aa bb cc", "aa bb bb aa"), 2)
    val rdd2: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_,1))
    println(s"rdd2分区数：${rdd2.getNumPartitions}")
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _, 4)
    println(s"rdd3分区数：${rdd3.getNumPartitions}")
    rdd3.count()


  }
}
