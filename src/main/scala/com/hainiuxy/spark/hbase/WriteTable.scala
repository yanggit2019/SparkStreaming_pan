package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WriteTable")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(10 to 20, 2)

    // 一个分区创建hbase连接，一条一条写入
    // Int -->(NullWritable, Put)
    val writeRdd: RDD[(NullWritable, Put)] = rdd.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_write_${f}"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
      (NullWritable.get(), put)
    })

    // 创建带有hbase连接的配置对象
    val hbaseConf: Configuration = HBaseConfiguration.create()

    // 配置表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "panniu:spark_user")

    // 配置outputformatclass
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[NullWritable]].getName)

    // 写入hbase表
    writeRdd.saveAsNewAPIHadoopDataset(hbaseConf)

  }

}
