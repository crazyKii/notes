package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值
 */
object Spark10_Transformation_glom {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)
    println("------------没有glom之前------------")
    rdd.mapPartitionsWithIndex((index,datas) => {
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()

    println("------------调用glom之后------------")
    val newRDD: RDD[Array[Int]] = rdd.glom()
    newRDD.mapPartitionsWithIndex((index,datas) => {
      // println(index + "--->" + datas.mkString(","))
      // datas是数组，分区只有一个数组，直接用next获取到数组
      println(index + "--->" + datas.next().mkString(","))
      datas
    }).collect()

    // 关闭连接
    sc.stop()
  }
}
