package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个RDD，按照元素模以2的值进行分组
 */
object Spark11_Transformation_groupBy {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),2)
    println("==============groupBy分组前================")

    rdd.mapPartitionsWithIndex((index,datas) => {
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()

    println("------------groupBy分组后------------")
    val newRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 3)
    newRDD.mapPartitionsWithIndex((index,datas) => {
      println(index + "--->" + datas.mkString(","))
      datas
    }).collect()


    /*
    val rdd: RDD[String] = sc.makeRDD(List("atguigu","hello","scala","atguigu","hello","scala"))
    val newRDD: RDD[(String, Iterable[String])] = rdd.groupBy(elem=>elem)
    newRDD.collect().foreach(println)
    */

    // 通过4040看本地执行效果(http://localhost:4040/)
    Thread.sleep(100000000)

    // 关闭连接
    sc.stop()
  }
}
