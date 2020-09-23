package com.hainiuxy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

object SparkSQLSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLSchema")

    val sc = new SparkContext(conf)
    // 创建SQLContext
    val sqlc = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile("E:\\tmp\\sparksql\\input_text")
    // 一行的字符串变成 Row
    // CN	game	cn.gameloft.aa	1
    val rowRdd: RDD[Row] = rdd.map(f => {
      val arr: Array[String] = f.split("\t")
      val country: String = arr(0)
      val gpcategory: String = arr(1)
      val pkgname: String = arr(2)
      val num: Int = arr(3).toInt

      Row(country, gpcategory, pkgname, num)
    })

    val fields = new ArrayBuffer[StructField]
    fields += new StructField("country", DataTypes.StringType, true)
    fields += new StructField("gpcategory", DataTypes.StringType, true)
    fields += new StructField("pkgname", DataTypes.StringType, true)
    fields += new StructField("num", DataTypes.IntegerType, true)
    // Rdd[Row]  的 Row中对应的字段类型
    val structType: StructType = StructType(fields)
    // 构建dataframe
    val df: DataFrame = sqlc.createDataFrame(rowRdd, structType)

    df.printSchema()
    df.show()

    // 统计country是CN的记录数
    val filterDF: Dataset[Row] = df.filter(df("country").like("%CN%"))
    val num: Long = filterDF.count()
    println(s"country是CN的记录数:${num}")
  }
}
