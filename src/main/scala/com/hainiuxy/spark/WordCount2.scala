package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount2")

    val sc = new SparkContext(sparkConf)

    // 自己设定用户期望的分区数5
    val rdd: RDD[String] = sc.textFile("E:\\tmp\\spark\\input", 5)

    println(s"partition num:${rdd.getNumPartitions}")

    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split("\t")).map((_,1)).reduceByKey(_ + _)

    val arr: Array[(String, Int)] = resRdd.collect()
    println(arr.toBuffer)

  }
}
