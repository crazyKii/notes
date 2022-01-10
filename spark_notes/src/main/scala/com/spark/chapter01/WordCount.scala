package com.spark.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    /*
    // 创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    // 创建sparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 读取文件
    val textRDD: RDD[String] = sc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/input")
    // 对读取的内容，进行切割，并进行扁平化
    val flatMapRDD: RDD[String] = textRDD.flatMap(_.split(" "))
    // 对数据集中内容进行转化 ===> 计数
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    // 给相同单词出现的次数进行汇总
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    // 将执行的结果进行收集
    val res: Array[(String, Int)] = reduceRDD.collect()
    res.foreach(println)
    */

    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    // 释放资源
    sc.stop()
  }


}
