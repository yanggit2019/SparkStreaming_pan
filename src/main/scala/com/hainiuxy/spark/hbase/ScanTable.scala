package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScanTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ScanTable")
    val sc = new SparkContext(conf)


    // 参考TableMapReduceUtil.initTableMapperJob 的参数设置的
    // 创建hbase配置对象
    val hbaseConf: Configuration = HBaseConfiguration.create()

    // 设置表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "panniu:spark_user_split");

    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes("spark_write_3"))
    scan.setStopRow(Bytes.toBytes("spark_write_7"))

    // 设置查询范围
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));

    // ImmutableBytesWritable:代表一行的rowkey
    // Result：代表一行的数据
//    conf: Configuration = hadoopConfiguration,
//    fClass: Class[F],   inputFormatClass
//    kClass: Class[K],   keyinclass
//    vClass: Class[V]    valueinclass
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // rdd的分区数，是由你读取hbase表数据的region数决定的
    println(s"rdd分区数：${rdd.getNumPartitions}")

    rdd.foreach(f =>{
      val rowkey: String = Bytes.toString(f._1.get())
      val bytes: Array[Byte] = f._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count"))
      val value = Bytes.toString(bytes)
      println(s"rowkey=${rowkey}\tcolumn=cf:count, value=${value}")

    })

  }

}
