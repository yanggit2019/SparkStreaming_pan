package com.hainiuxy.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object SparkSessionRdd2DF2DS9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")

    // 创建sparksession
    val sparkSession: SparkSession = SparkSession.builder().config(conf)
      .appName("SparkSessionRdd2DF2DS9")
      .getOrCreate()

    val rdd: RDD[(String, Int)] = sparkSession.sparkContext.parallelize(List(("aa",1),("bb",2),("aa",3),("bb", 4)), 2)

    // 引入隐式转换
    // 给rdd赋予 DatasetHolder的能力，DatasetHolder 里面有toDF，toDS方法
    import sparkSession.implicits._

    // rdd ---> df
    val df: DataFrame = rdd.toDF("word", "num")
    df.printSchema()
    df.show()

    // rdd --->ds
    val ds: Dataset[(String, Int)] = rdd.toDS()
    ds.printSchema()
    ds.show()

    // df --->rdd
    val rdd2: RDD[Row] = df.rdd
    // ds ---> rdd
    val rdd3: RDD[(String, Int)] = ds.rdd
    val map: collection.Map[String, Int] = rdd3.reduceByKey(_ + _).collectAsMap()
    println(map)

    val map2: collection.Map[String, Int] = rdd2.map(f => {
      (f.getString(0), f.getInt(1))
    }).reduceByKey(_ + _).collectAsMap()
    println(map2)

    // rdd ---> ds
    val beanDS: Dataset[HainiuBean] = rdd.map(f => HainiuBean(f._1,f._2)).toDS()
    beanDS.map(bean =>{
      println(s"${bean.word}\t${bean.num}")
      bean
    }).count()

  }
}

//定义样例类
case class HainiuBean(val word:String, val num:Int)