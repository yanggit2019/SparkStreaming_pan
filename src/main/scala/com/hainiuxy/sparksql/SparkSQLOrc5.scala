package com.hainiuxy.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLOrc5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLOrc5")
    conf.set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    // 创建HiveContext
    val hivec = new HiveContext(sc)

    // 读取orc文件
    val df: DataFrame = hivec.read.orc("E:\\tmp\\sparksql\\input_orc")

    df.printSchema()
    // 查询前5条数据
    df.show(5)
    // select * from (select count(*) as num from xxx group by country) t where t.num > 5
    df.createOrReplaceTempView("orc_table")

    val df2: DataFrame = hivec.sql("select * from (select country, count(*) as num from orc_table group by country) t where t.num > 5")


    //缓存DataSet
    // storageLevel: StorageLevel = MEMORY_AND_DISK
    val cacheDS: Dataset[Row] = df2.cache()


    // 以overwrite方式写入orc格式文件
    cacheDS.write.mode(SaveMode.Overwrite).format("orc").save("/tmp/sparksql/output_orc")

    // 以overwrite方式写入json格式文件
    cacheDS.write.mode(SaveMode.Overwrite).format("json").save("/tmp/sparksql/output_json")
  }
}
