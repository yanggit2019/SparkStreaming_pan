package com.hainiuxy.sparksql

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.hive.jdbc.HiveDriver

object SparkJdbcThriftServer {
  def main(args: Array[String]): Unit = {

    // 加载hive驱动
    classOf[HiveDriver]

    var conn:Connection = null
    var stmt: Statement = null

    try{
      // 创建连接
      conn = DriverManager.getConnection("jdbc:hive2://op.hadoop:20000/panniu","","")

      // 创建statement对象
      stmt = conn.createStatement()

      val sql:String = "select sum(num) from (select count(*) as num from user_install_status group by country) a"

      // 设置shuffle分区数
      // 验证时，要把跟这个SQL相关的缓存干掉，否则会影响分区
      stmt.execute("set spark.sql.shuffle.partitions=2")

      // 通过SQL得到查询结果
      val rs: ResultSet = stmt.executeQuery(sql)

      while(rs.next()){
        val num: Int = rs.getInt(1)
        println(num)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      stmt.close()
      conn.close()
    }





  }
}
