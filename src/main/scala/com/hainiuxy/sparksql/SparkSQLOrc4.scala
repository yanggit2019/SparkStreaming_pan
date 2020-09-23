package com.hainiuxy.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object SparkSQLOrc4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLOrc4")
    conf.set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    // 创建SQLContext
    val sqlc = new SQLContext(sc)

    // 读取orc文件
    val df: DataFrame = sqlc.read.orc("E:\\tmp\\sparksql\\input_orc")

    df.printSchema()
    // 查询前5条数据
    df.show(5)
    // select * from (select count(*) as num from xxx group by country) t where t.num > 5
    // 按照country统计
    val countDF: DataFrame = df.groupBy(df("country")).count()

    // 查询给count 起别名 num
    val aliasDF: DataFrame = countDF.select(countDF("country"), countDF("count").alias("num"))
    // 筛选 num > 5
    val filterDS: Dataset[Row] = aliasDF.filter(aliasDF("num") > 5)

    //缓存DataSet
    // storageLevel: StorageLevel = MEMORY_AND_DISK
    val cacheDS: Dataset[Row] = filterDS.cache()


    // 以overwrite方式写入orc格式文件
    cacheDS.write.mode(SaveMode.Overwrite).format("orc").save("/tmp/sparksql/output_orc")

    // 以overwrite方式写入json格式文件
    cacheDS.write.mode(SaveMode.Overwrite).format("json").save("/tmp/sparksql/output_json")
  }
}
