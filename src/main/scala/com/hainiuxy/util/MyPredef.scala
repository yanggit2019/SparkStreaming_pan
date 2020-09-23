package com.hainiuxy.util

object MyPredef {

  // 定义隐式转换函数
  implicit val string2HdfsUtil = (path:String) => new HdfsUtil(path)
}
