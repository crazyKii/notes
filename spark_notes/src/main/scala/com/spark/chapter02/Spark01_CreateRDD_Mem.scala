package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_CreateRDD_Mem {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 方式1： 通过读取内存集合中的数据，创建RDD
    // 1、创建一个集合对象
    val list: List[Int] = List(2, 3, 4, 5)

    // 2、根据集合创建RDD
    // 使用parallelize
    // val rdd: RDD[Int] = sc.parallelize(list)
    // 使用makeRDD
    val rdd: RDD[Int] = sc.makeRDD(list)

    // 3、输出
    rdd.collect().foreach(println)

    // 关闭
    sc.stop()
  }
}
