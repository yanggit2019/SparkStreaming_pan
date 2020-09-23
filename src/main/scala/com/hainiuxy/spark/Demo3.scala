package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Demo3")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.parallelize(List((1,1),(5,5),(3,3),(7,7),(4,4),(9,9)),3)
//    rdd.mapPartitionsWithIndex((partitionId, it) =>{
//      println(s"id:${partitionId}, it:${it.toList}")
//      it
//    }).count()

    // 用 RangePartitioner 重新分区后，分区间有序，分区内无序
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new RangePartitioner(2,rdd))
        rdd2.mapPartitionsWithIndex((partitionId, it) =>{
          println(s"id:${partitionId}, it:${it.toList}")
          it
        }).count()




  }
}
