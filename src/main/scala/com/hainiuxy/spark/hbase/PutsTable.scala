package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PutsTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("putstable")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(10 to 20, 2)

    // 一个分区创建一个连接， 一个分区的数据批量写入
    rdd.foreachPartition(it =>{
      // 加载hbase配置
      val hbaseConf: Configuration = HBaseConfiguration.create()
      // 创建hbase连接
      val conn: Connection = ConnectionFactory.createConnection(hbaseConf)
      // 创建hbase表操作对象
      val table: HTable = conn.getTable(TableName.valueOf("panniu:spark_user")).asInstanceOf[HTable]

      // 旧it --> 新it
      val puts: Iterator[Put] = it.map(f => {
        val put = new Put(Bytes.toBytes(s"spark_puts_${f}"))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
        put
      })

      val list: List[Put] = puts.toList

      // 通过隐式转换，把scala的list转成java的list
      import scala.collection.convert.wrapAll._
      // 一个分区批量写入
      table.put(list)

      table.close()
      // 关闭连接
      conn.close()
    })
  }

}
