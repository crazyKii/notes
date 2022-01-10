package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
 */
object Spark08_Transformation_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 8, 3)

    val newRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    newRDD.collect().foreach(println)

    // 第二个分区元素*2，其余分区不变
    val newRDD2: RDD[Int] = rdd.mapPartitionsWithIndex((index, datas) => {
      /*
      if (index == 1)
        datas.map(_ * 2)
      else
        datas
      */
      // 模式匹配
      index match {
        case 1 => datas.map(_ * 2)
        case _ => datas
      }
    })
    newRDD2.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
