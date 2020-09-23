package com.hainiuxy.exam


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo1")
    conf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("E:\\tmp\\spark\\exam\\input1")
    val rowRdd: RDD[Row] = rdd.map(f => {
      val arr: Array[String] = f.split(" ")
      Row(arr(0), arr(1))
    })

    val sqlc = new SQLContext(sc)
    val fields = new ArrayBuffer[StructField]
    fields += new StructField("ip", DataTypes.StringType, true)
    fields += new StructField("name", DataTypes.StringType, true)
    val structType: StructType = StructType(fields)
    val df: DataFrame = sqlc.createDataFrame(rowRdd, structType)
    df.createOrReplaceTempView("iptable")

//    val df2: DataFrame = sqlc.sql("select ip, count(distinct name) as mum from iptable group by ip")
    val df2: DataFrame = sqlc.sql("select t.ip, count(t.name) as num from (select ip, name from iptable group by ip, name)t group by t.ip")
    df2.printSchema()
    df2.show()

  }
}
