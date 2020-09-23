package com.hainiuxy.sparksql

import java.util.Properties

import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSessionUDF10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")

    // 创建sparksession
    val sparkSession: SparkSession = SparkSession.builder().config(conf)
      .appName("SparkSessionUDF10")
      .getOrCreate()

//  获取SparkContext对象：  sparkSession.sparkContext

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","root")
    // 密码
    prop.setProperty("password","111111")

    val s1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/hainiu_test","student", prop)

    // 注册udf函数
    // name_length ： 函数名
    // (f:String) => f.length: 要执行的函数的逻辑
    sparkSession.udf.register("name_length", (f:String) => f.length)

    s1.createOrReplaceTempView("student")
    // 使用udf函数
    val df: DataFrame = sparkSession
      .sql("select s_name, name_length(s_name) from student")
    df.printSchema()
    df.show()



  }
}
