package com.hainiuxy.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SharedVarTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sharedvartest")
    val sc = new SparkContext(conf)
    //driver端定义外部变量list
    val list = List(1,5,8)

    // 用广播变量包装外部变量
    val broad: Broadcast[List[Int]] = sc.broadcast(list)

    // 定义累加器
    val countAcc: LongAccumulator = sc.longAccumulator

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8), 2)

    val filter: RDD[Int] = rdd.filter( x => {
      // 从广播变量中提取list
      val list2: List[Int] = broad.value
      // executor 端用于计算和统计
      if(list2.contains(x)) {
        // 累加器累加值
        countAcc.add(1)
        true
      }else{
        false
      }

    })

    val rs: Array[Int] = filter.collect()

    // 提取累加器的汇总结果
    println(s"count1:${countAcc.value}")
    println(rs.toBuffer)
    filter.count()
    // 当统计时，小心累加器重复累加
    println(s"count2:${countAcc.value}")
  }
}
