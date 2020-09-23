package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("ForeachDemo")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(List(1,3,2,4,5,6),2)
//    rdd.foreach(println)

    rdd.foreachPartition(it =>{
      // it: 每个分区的数据的集合
      // it.foreach 是代表遍历分区集合的数据，是scala的方法
      it.foreach(f =>{
        println(s"data:${f}")
      })
    })

  }
}
