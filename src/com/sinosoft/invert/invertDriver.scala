package com.sinosoft.invert

import org.apache.spark.{SparkConf, SparkContext}

object invertDriver {
  def main(args: Array[String]): Unit = {
    //倒排索引，查找关键词所在文件
    val conf = new SparkConf().setMaster("local").setAppName("invert")
    val sc = new SparkContext(conf)
    val data = sc.wholeTextFiles("d://Spark//data//inverted/*", 2)
    //将指定目录下的所有文件读取到一个RDD中
    val r1 = data.map { case (filePath, fileText) =>
      // 在路径中获取文档名
      val fileName = filePath.split("/").last.dropRight(4)
      (fileName, fileText)
    }

    val r2 = r1.flatMap { case (fileName, fileText) =>
      //在切分单词时：先按\r\n切出行数据,再按空格切单词
      fileText.split("\r\n")
        .flatMap { line =>
          line.split(" ")
            .map { word => (word, fileName) }
        }
    }

    val r3 = r2.groupByKey.map { case (word, buffer) => (word, buffer.toList.distinct) }
      .map { case (word, list) => (word, list.mkString(",")) }
    r3.foreach {
      println
    }
  }
}
