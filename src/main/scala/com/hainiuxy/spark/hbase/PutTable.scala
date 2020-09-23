package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PutTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("puttable")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(10 to 20, 2)

    // 一条数据创建hbase连接，创建表操作对象，一条一条写入，效率低
    rdd.foreach(f =>{
      // 加载hbase配置
      val hbaseConf: Configuration = HBaseConfiguration.create()
      // 创建hbase连接
      val conn: Connection = ConnectionFactory.createConnection(hbaseConf)
      // 创建hbase表操作对象
      val table: HTable = conn.getTable(TableName.valueOf("panniu:spark_user")).asInstanceOf[HTable]

      val put = new Put(Bytes.toBytes(s"spark_put_${f}"))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
      // 写入hbase表
      table.put(put)

      table.close()
      // 关闭连接
      conn.close()
    })
  }

}
