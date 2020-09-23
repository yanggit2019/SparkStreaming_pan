package com.hainiuxy.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
class SparkBulkloadTable
object SparkBulkloadTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("SparkBulkloadTable")
    // 开启Kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val tableName:String = args(1)
//    val tableName:String = "panniu:spark_load"
    val outputPath:String = args(0)
//    val outputPath:String = "/tmp/spark/hbase_bulk_output"

    val rdd: RDD[Int] = sc.parallelize(List(1,6,3,8,3,5,8,9), 2)
    // num --> (ImmutableBytesWritable, keyvalue)
    val pairRdd: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.map(f => {
      val rowkey: String = s"spark_load_${f}"
      val bytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowkey))
      val keyValue = new KeyValue(Bytes.toBytes(rowkey),
        Bytes.toBytes("cf"),
        Bytes.toBytes("count"),
        Bytes.toBytes(s"${f}"))
      (bytesWritable, keyValue)
    })

    // rowkey有序
    val sortRdd: RDD[(ImmutableBytesWritable, KeyValue)] = pairRdd.sortByKey()

    val hbaseConf: Configuration = HBaseConfiguration.create()
    // 创建Job对象
    val job: Job = Job.getInstance(hbaseConf)
    val conn: Connection = ConnectionFactory.createConnection(hbaseConf)

    val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    // 设置生成hfile文件的参数
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator())

    import com.hainiuxy.util.MyPredef.string2HdfsUtil
    outputPath.deleteHdfs

    sortRdd.saveAsNewAPIHadoopFile(outputPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    //--------集成导入hbase表的逻辑-----------------------
    //hadoop jar /usr/local/hbase/lib/hbase-shell-1.3.1.jar completebulkload /user/panniu/spark/hbase_bulk_output panniu:spark_load
    // 参考 上面的命令，调用LoadIncrementalHFiles.main() 实现导入HBASE表
    // 【注意】导入逻辑必须在集群上运行
    LoadIncrementalHFiles.main(Array(outputPath, tableName))
  }

}
