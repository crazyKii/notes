package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 执行-pipe
 */
object Spark18_Transformation_pipe {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD (List("hi","Hello","how","are","you"),1)

    rdd.pipe("/opt/module/spark/pipe.sh").collect()

    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
