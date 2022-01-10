package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个1-4数组的RDD，两个分区，将所有元素*2形成新的RDD
 */
object Spark07_Transformation_mapPartitions {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 以分区为单位，对RDD中的元素进行映射
    // 一般适用于批处理的操作，比如：将RDD中的元素插入到数据库中，需要数据库连接，
    // 如果每一个元素都创建一个连接，效率很低；可以对每个分区的元素，创建一个连接
    val newRDD: RDD[Int] = rdd.mapPartitions(datas => {
      datas.map(_ * 2)
    })
    // 简写
    // val newRDD: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))

    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
