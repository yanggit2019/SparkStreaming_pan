package com.hainiuxy.sparksql

import java.util.Properties

import com.mysql.jdbc.Driver
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object SparkSessionJDBC8 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")

    // 创建sparksession
    val sparkSession: SparkSession = SparkSession.builder().config(conf)
      .appName("SparkSessionJDBC8")
      .getOrCreate()

//  获取SparkContext对象：  sparkSession.sparkContext

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","root")
    // 密码
    prop.setProperty("password","111111")

    val s1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test","student", prop)
//    val s2: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test","student_course", prop)

    val s2: DataFrame = sparkSession.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://localhost:3306/hainiu_test")
      .option("dbtable", "student_course")
      .option("user", "root")
      .option("password", "111111").load()


    s1.createOrReplaceTempView("s")
    s2.createOrReplaceTempView("sc")

    val joinDF: DataFrame = sparkSession.sql("select * from s INNER JOIN sc on s.S_ID=sc.SC_S_ID")
    joinDF.printSchema()
    joinDF.show()

    //保存
    val saveoptions = Map("header" -> "true", "delimiter" -> ",", "path" -> "/tmp/sparksql/output_csv")
    joinDF.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveoptions).save()

  }
}
