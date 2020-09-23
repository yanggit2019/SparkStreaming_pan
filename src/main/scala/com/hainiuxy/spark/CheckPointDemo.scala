package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("CheckPointDemo")
    val sc = new SparkContext(conf)

    val checkPointPath = "/tmp/spark/check"
    // 设置checkpoint目录
    sc.setCheckpointDir(checkPointPath)

    val rdd: RDD[Int] = sc.parallelize(List(1,3,2,4,5,6),2)
    val rdd2: RDD[Int] = rdd.map(f => {
      println(s"data:${f}")
      f * 10
    })

    val cache: RDD[Int] = rdd2.cache()

    // checkpoint是转换算子，当执行时，会重新执行上面的操作，这样就多执行了一遍。
    // 解决方案：在checkpoint 上面 执行 cache

    // 当进行错误恢复时，会向上追溯，当追溯到checkpoint时，就不追溯了，
    // 因为执行完checkpoint，rdd会忘记血缘关系
    cache.checkpoint()

    cache.count()



  }
}
