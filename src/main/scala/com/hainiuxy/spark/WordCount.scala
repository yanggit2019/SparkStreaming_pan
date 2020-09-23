package com.hainiuxy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("wordcountforscala")

    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("C:\\Users\\My\\Desktop\\spark\\input")

//    println(s"partition num:${rdd.getNumPartitions}")
//     groupBy + mapValues 组合，效率低，groupBy只做汇总，但不计算
//    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.size)
    // reduceByKey : 效率高， 按照单词汇总value，并计算value的值
    val resRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

//    val arr: Array[(String, Int)] = resRdd.collect()
//    println(arr.toBuffer)

    val outputPath:String = "C:\\Users\\My\\Desktop\\spark\\output"

//    val hadoopConf = new Configuration()
//    val fs: FileSystem = FileSystem.get(hadoopConf)
//    val outputDir = new Path(outputPath)
//    if(fs.exists(outputDir)){
//      fs.delete(outputDir,true)
//      println(s"delete outputpath:${outputPath}")
//    }
    // 通过隐式转换给字符串赋予删除hdfs的功能
    import com.hainiuxy.util.MyPredef.string2HdfsUtil
    outputPath.deleteHdfs

    // 把rdd的数据以Text的格式写入到hdfs
    resRdd.saveAsTextFile(outputPath)

  }
}
