package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteTableWithRepartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WriteTableWithRepartition")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(30 to 90, 10)

    // 一个分区创建hbase连接，一条一条写入
    // Int -->(NullWritable, Put)
    val writeRdd: RDD[(NullWritable, Put)] = rdd.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_write_p_${f}"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
      (NullWritable.get(), put)
    })

    println(s"重新分区前：${writeRdd.getNumPartitions}")

    // 后面sparkSQL的group by 操作，默认产生200个分区，如果数据量不大，没必要搞200分区，
    // 所以可以通过 coalesce 来减少分区
    val repartitonRdd: RDD[(NullWritable, Put)] = writeRdd.coalesce(2)

    println(s"重新分区后：${repartitonRdd.getNumPartitions}")

    // 创建带有hbase连接的配置对象
    val hbaseConf: Configuration = HBaseConfiguration.create()

    // 配置表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "panniu:spark_user")

    // 配置outputformatclass
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[NullWritable]].getName)

    // 写入hbase表
    repartitonRdd.saveAsNewAPIHadoopDataset(hbaseConf)

  }

}
