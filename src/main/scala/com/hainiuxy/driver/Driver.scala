package com.hainiuxy.driver

import com.hainiuxy.html_text.NewsExtractorStreaming
import com.hainiuxy.spark.MapJoin
import com.hainiuxy.spark.hbase.SparkBulkloadTable
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("mapjoin", classOf[MapJoin],"字典文件与orc文件join")
    driver.addClass("spark_load_hbase", classOf[SparkBulkloadTable],"生成hfile文件，导入HBASE表")
    driver.addClass("news_extractor", classOf[NewsExtractorStreaming],"正文抽取")
    driver.run(args)
  }
}
