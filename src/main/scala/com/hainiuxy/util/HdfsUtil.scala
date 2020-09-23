package com.hainiuxy.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsUtil(val path:String) {

  def deleteHdfs = {
    // 具体删除hdfs的逻辑
    val hadoopConf = new Configuration()
    val fs: FileSystem = FileSystem.get(hadoopConf)
    val outputDir = new Path(path)
    if(fs.exists(outputDir)){
      fs.delete(outputDir,true)
      println(s"delete outputpath:${path}")
    }
  }
}
