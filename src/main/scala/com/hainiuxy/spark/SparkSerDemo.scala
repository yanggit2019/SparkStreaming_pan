package com.hainiuxy.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.Text
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[UserInfo])
    kryo.register(classOf[Text])
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))
    kryo.register(classOf[Array[String]])
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(Class.forName("java.lang.Class"))
    kryo.register(classOf[Array[UserInfo]])

  }
}

object SparkSerDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSerDemo")
    // 开启Kryo序列化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 标明要主动注册序列化类
    sparkConf.set("spark.kryo.registrationRequired", "true")

//    // 注册序列化类方案1
//    val classes: Array[Class[_]] = Array[Class[_]](classOf[UserInfo],
//      classOf[Text],
//      Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
//      classOf[Array[String]],
//      Class.forName("scala.reflect.ClassTag$$anon$1"),
//      Class.forName("java.lang.Class"),
//      classOf[Array[UserInfo]]
//     )
//    // 将上面的类注册
//    sparkConf.registerKryoClasses(classes)

    // 方案2： 自定义序列化注册类，配置注册类
    sparkConf.set("spark.kryo.registrator",classOf[MyRegistrator].getName)

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.parallelize(List("aa","bb","aa","aa"),2)

    val user = new UserInfo

    val broad: Broadcast[UserInfo] = sc.broadcast(user)

    // 如果 UserInfo 对象没有实现序列化，在执行shuffle操作时会报错
    val arr: Array[(String, Iterable[UserInfo])] = rdd.map(f => {
      val userInfo: UserInfo = broad.value
      (f, userInfo)
    }).groupByKey().collect()

    for(t <- arr){
      println(t)
    }


  }
}
