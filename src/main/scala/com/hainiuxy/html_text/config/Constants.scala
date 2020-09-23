package com.hainiuxy.html_text.config

object Constants {

  val KAFKA_CONSUMER_GROUP:String = "c20_pan_group003"

  val KAFKA_CONFIG_MAP:Map[String,String] = Map(
    "topic" -> "hainiu_html",
    "brokers" -> "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092",
    "zk_client" -> "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"
  )

  val CONFIG_PATH:String = "/tmp/spark/xpath_cache_file"
//  val CONFIG_PATH:String = "/user/panniu/xpath_cache_file"

  // HBASE表名称
  val HASE_TABLE_NAME: String = "panniu:context_extract"

  // es 索引库类型
  val ES_INDEX_TYPE:String = "panniu_hainiu_spark/news"

  // es 连接节点名称
  val ES_NODE_NAME:String = "s1.hadoop"

  // MySQL数据库连接
  val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://localhost/hainiu_test", "username" -> "root", "password" -> "111111")
//  val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://192.168.88.195/hainiutest", "username" -> "hainiu", "password" -> "12345678")


  // 统计表
  val MYSQL_REPORT_EXTRACT:String = "report_stream_extract"
//  val MYSQL_REPORT_EXTRACT:String = "panniu_report_stream_extract"

}
