package com.hainiuxy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object SparkSQLJson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLJson")

    val sc = new SparkContext(conf)
    // 创建SQLContext
    val sqlc = new SQLContext(sc)

    // 通过读取json文件构建DataFrame
    val df: DataFrame = sqlc.read.json("E:\\tmp\\sparksql\\input_json")
    // 查看DataFrame中的内容
    df.show()

    // 查看schema信息
    df.printSchema()

    // 查看country列中的内容
    df.select(df.col("country")).show()
    df.select(df("country")).show()
    df.select("country").show()

    // 查询所有country和num，并把num+1
    val df2: DataFrame = df.select(df("country"), (df("num") + 1).as("num1"))
    df2.show()
    // 查询num < 2 的数据
    val df3: Dataset[Row] = df.filter(df("num") < 2)
    df3.show()
    // 按照country 统计相同country的数量
    val groupDF: DataFrame = df.groupBy(df("country")).count()
    groupDF.printSchema()

    // 将统计后的结果保存到hdfs上
    // df --> rdd
    val rdd: RDD[Row] = groupDF.rdd

    val rdd2: RDD[Row] = rdd.coalesce(2)

//    root
//    |-- country: string (nullable = true)
//    |-- count: long (nullable = false)
    val rdd3: RDD[String] = rdd2.map(row => {
      s"${row.getString(0)}\t${row.getLong(1)}"
    })

    val outputPath:String = "/tmp/sparksql/output_txt"
    import com.hainiuxy.util.MyPredef.string2HdfsUtil
    outputPath.deleteHdfs

    rdd3.saveAsTextFile(outputPath)

  }
}
