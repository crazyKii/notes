package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_CreateRDD_File {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 方法2 ： 从外部存储系统的数据集创建
    // val rdd: RDD[String] = sc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/input/1.txt")
    val rdd: RDD[String] = sc.textFile("hdfs://node101:8020/input")

    // 打印
    rdd.foreach(println)

    // 关闭
    sc.stop()
  }

}
