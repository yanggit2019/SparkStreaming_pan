package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 自定义二次排序的key，需要实现比较逻辑和序列化
class SecondaryKey(val word:String, val num:Int) extends Ordered[SecondaryKey] with Serializable {
  // 二次比较的逻辑, 按照单词降序，按照数值升序
  override def compare(that: SecondaryKey): Int = {
    if(this.word.compareTo(that.word) == 0){
      this.num - that.num
    }else{
      // 单词降序
      that.word.compareTo(this.word)
    }

  }
}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("secondarysort")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("E:\\tmp\\spark\\secondary_sort\\input")
    val filterRdd: RDD[String] = rdd.filter(f => {
      if (f.startsWith("(") && f.endsWith(")")) true else false
    })
    val pairRdd: RDD[(SecondaryKey, String)] = filterRdd.map(f => {
      // "(hainiu,100)" --> (SecondaryKey,"hainiu\t100")
      val tmp: String = f.substring(1, f.length - 1)
      val arr: Array[String] = tmp.split(",")
      val word: String = arr(0)
      val num: Int = arr(1).toInt
      (new SecondaryKey(word, num), s"${word}\t${num}")
    })
    // sortByKey 会执行shuffle，需要序列化
    val arr: Array[(SecondaryKey, String)] = pairRdd.sortByKey().collect()
    for(t <- arr){
      println(t._2)
    }
  }
}
