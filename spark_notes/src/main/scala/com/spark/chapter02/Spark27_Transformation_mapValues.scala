package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 转换算子-mapValues
 *   -对kv类型的RDD中的value部分进行映射
 */
object Spark27_Transformation_mapValues {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    rdd.mapValues("||"+_).collect().foreach(println)
    //Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
