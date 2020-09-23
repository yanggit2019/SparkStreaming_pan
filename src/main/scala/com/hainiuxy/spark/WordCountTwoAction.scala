package com.hainiuxy.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountTwoAction {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountTwoAction")

    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("C:\\Users\\My\\Desktop\\spark\\input")
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMapRdd.map(f => {
      println(s"data:${f}")
      (f, 1)
    })


    // 当有多个action算子时，你不想重新计算，就可以把rdd的转换结果cache到内存
    // 这样，第二个action算子执行时，向上追溯到cache就不追溯了
    // cache 默认是把数据缓存到内存 MEMORY_ONLY
    // cache 是个转换算子
    val cache: RDD[(String, Int)] = mapRdd.cache()

    val arr: Array[(String, Int)] = cache.collect()
    println(arr.toBuffer)

    cache.count()
  }
}
