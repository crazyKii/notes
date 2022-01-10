package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：对数据进行去重
 */
object Spark15_Transformation_distinct {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    //创建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,5,4,3,3),5)
    numRDD.mapPartitionsWithIndex((index,datas) => {
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()
    println("------------------------------")

    val newRDD: RDD[Int] = numRDD.distinct()
    newRDD.mapPartitionsWithIndex((index,datas) => {
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()
    // 对比每个分区的数据，发现数据变了位置，说明发生了shuffle

    // 对RDD采用多个Task去重，提高并发度，设置分区
    numRDD.distinct(2).collect.foreach(println)
    // 关闭连接
    sc.stop()
  }
}
