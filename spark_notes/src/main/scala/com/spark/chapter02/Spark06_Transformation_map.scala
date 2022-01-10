package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个1-4数组的RDD，两个分区，将所有元素*2形成新的RDD
 */
object Spark06_Transformation_map {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // val newRDD: RDD[Int] = rdd.map(item => {item * 2})
    val newRDD: RDD[Int] = rdd.map(_ * 2)
    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
