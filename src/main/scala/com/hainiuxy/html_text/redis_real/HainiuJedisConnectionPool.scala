package com.hainiuxy.html_text.redis_real

import java.{lang, util}

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object HainiuJedisConnectionPool {

  private val config = new JedisPoolConfig
  //最大的连接数
  config.setMaxTotal(20)

  //最大的空闲数
  config.setMaxIdle(10)

  //保持连接活跃
  config.setTestOnBorrow(true)


  //访问带密码的redis库
  //private val pool = new JedisPool(config,"nn1.hadoop",6379,10000,"hainiu666")
  private val pool = new JedisPool(config, "nn1.hadoop", 6379, 10000)

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn: Jedis = HainiuJedisConnectionPool.getConnection()
//    conn.set("p_hainiu", "1000")
//    val v1: String = conn.get("p_hainiu")
//
//    println(v1)
//
    conn.incrBy("p_total:host", 1)
    val v2: String = conn.get("p_total:host")
    println(v2)

    conn.zincrby("p_txpath:host", 1d, "xpath1")
    conn.zincrby("p_txpath:host", 2d, "xpath2")
    conn.zincrby("p_txpath:host", 3d, "xpath3")

    // 获取 对应的分值
    val x1: lang.Double = conn.zscore("p_txpath:host", "xpath1")
    val x2: lang.Double = conn.zscore("p_txpath:host", "xpath2")
    val x3: lang.Double = conn.zscore("p_txpath:host", "xpath3")
    println(s"xpath1:${x1}, xpath2:${x2}, xpath3:${x3}")
    // 按照key得到的数据降序排序，取前两个
    val set: util.Set[String] = conn.zrevrange("p_txpath:host", 0, 1)
    println(set)

    // 添加对应key的values
    conn.sadd("p_fxpath:host", "fxpath1", "fxapth2", "fxpath3")


    val fset: util.Set[String] = conn.smembers("p_fxpath:host")
    import scala.collection.JavaConversions._
    for(s <- fset){
      println(s)
    }

    conn.close()

    //客户端的使用
    //    val jedis = new Jedis("nn1.hadoop",6379,10000)
    //    jedis.auth("hainiu666")
    //    jedis.set("hainiu2","666")
    //    println(jedis.get("hainiu2"))
    //    jedis.close()
  }
}