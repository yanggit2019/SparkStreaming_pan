package com.hainiuxy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

// 定义bean对象
class RowBean(val country:String, val gpcategory:String, val pkgname:String, val num:Int){
  // 通过反射调用getCountry 的方式来获取字段数据和类型
  def getCountry = this.country
  def getGpcategory = this.gpcategory
  def getPkgname = this.pkgname
  def getNum = this.num
}

object SparkSQLSchemaBean {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLSchemaBean")

    val sc = new SparkContext(conf)
    // 创建SQLContext
    val sqlc = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile("E:\\tmp\\sparksql\\input_text")
    // 一行的字符串变成 RowBean类型
    val beanRdd: RDD[RowBean] = rdd.map(f => {
      val arr: Array[String] = f.split("\t")
      val country: String = arr(0)
      val gpcategory: String = arr(1)
      val pkgname: String = arr(2)
      val num: Int = arr(3).toInt

      new RowBean(country, gpcategory, pkgname, num)
    })

    // 通过 RowBean类型 来构建DataFrame
    val df: DataFrame = sqlc.createDataFrame(beanRdd, classOf[RowBean])
    df.printSchema()
    df.show()

    // 通过给DataFrame 创建临时视图，用SQL查询
    df.createOrReplaceTempView("beantable")

    val df2: DataFrame = sqlc.sql("select * from beantable where country like '%CN%'")

    val num: Long = df2.count()
    println(s"country是CN的记录数:${num}")

    val df3: DataFrame = sqlc.sql("select count(*) as count_num from beantable where country like '%CN%'")
    df3.show()

  }
}
