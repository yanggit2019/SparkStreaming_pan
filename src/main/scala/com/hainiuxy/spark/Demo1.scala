package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("demo1")
    val sc = new SparkContext(conf)

    // 把scala的集合变成rdd,这种方式一般用于测试
    // 如果不设置分区数，那他会按照当前的CPU核数作为分区数
    // 如果设置分区数，就会按照分区数分区
    val rdd: RDD[Int] = sc.parallelize(List(1,3,2,4,5,6),2)

    println(rdd.getNumPartitions)
    rdd.count()
//    val rdd: RDD[Int] = sc.parallelize(Array(23,12,48,56,45))
//    val num: Long = rdd.count()
//    val sum: Int = rdd.reduce(_ + _)
//    val avg: Double = sum.toDouble/num
//    println(avg)


  }
}
