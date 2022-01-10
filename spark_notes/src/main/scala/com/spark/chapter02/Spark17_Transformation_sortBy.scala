package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 转换算子-sortBy
 *   对RDD中的元素进行排序
 */
object Spark17_Transformation_sortBy {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(3,2,1,4,63,2,4,7))
    //升序排序
    //rdd.sortBy((x:Int) => {x}).collect.foreach(println)
    //rdd.sortBy(x => x,true).collect.foreach(println)
    //降序排序
    //rdd.sortBy(x => -x).collect.foreach(println)
    //rdd.sortBy(x => x,false).collect.foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("1","4","3","22"))
    //按照字符串字符字典顺序进行排序
    //strRDD.sortBy(elem=>elem).collect().foreach(println)
    strRDD.sortBy(_.toInt).collect().foreach(println)

    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
