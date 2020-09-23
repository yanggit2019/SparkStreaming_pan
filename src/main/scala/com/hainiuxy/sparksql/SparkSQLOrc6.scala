package com.hainiuxy.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLOrc6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLOrc6")
    conf.set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    // 创建HiveContext
    val hivec = new HiveContext(sc)

    // 创建数据库
    hivec.sql("create database if not exists class20")

    // 进入数据库
    hivec.sql("use class20")

    // 创建表
    hivec.sql(
      """
        |CREATE TABLE if not exists `user_orc`(
        |  `aid` string COMMENT 'from deserializer',
        |  `pkgname` string COMMENT 'from deserializer',
        |  `uptime` bigint COMMENT 'from deserializer',
        |  `type` int COMMENT 'from deserializer',
        |  `country` string COMMENT 'from deserializer',
        |  `gpcategory` string COMMENT 'from deserializer')
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
      """.stripMargin)
    // 导入数据
    hivec.sql("load data local inpath '/tmp/sparksql/input_orc' overwrite into table user_orc")

    // 查询
    val df: DataFrame = hivec.sql("select * from (select country, count(*) as num from user_orc group by country) t where t.num > 5")

    df.printSchema()
    df.show()
  }
}
