package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("demo1")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(1 to 100,2)
    println(rdd.getNumPartitions)
//    rdd.map(f => {
//      println("创建连接")
//      println(s"写入数据：${f}")
//      f
//    }).count()

    rdd.mapPartitionsWithIndex((partitionId, it) =>{
      // partitionId:rdd分区id
      // it: rdd每个分区的数据的集合,这是scala的迭代器
      println(s"创建连接：${partitionId}")
      it.foreach(f =>{
        println(s"写入数据：${f}")
      })
      it
    }).count()


  }
}
