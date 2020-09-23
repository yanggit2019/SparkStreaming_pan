package com.hainiuxy.sparksql

import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLJDBC7 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLJDBC7")
    conf.set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    // 创建SQLContext
    val sqlc = new SQLContext(sc)

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","root")
    // 密码
    prop.setProperty("password","111111")

//    被read.jdbc替代了
//    sqlc.jdbc("","")

    val s1: DataFrame = sqlc.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test","student", prop)
    val s2: DataFrame = sqlc.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test","student_course", prop)

    s1.createOrReplaceTempView("s")
    s2.createOrReplaceTempView("sc")

    val joinDF: DataFrame = sqlc.sql("select * from s INNER JOIN sc on s.S_ID=sc.SC_S_ID")
    joinDF.printSchema()
    joinDF.show()

  }
}
