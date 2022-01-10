package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：创建一个RDD（1-10），从中选择放回和不放回抽
 */
object Spark14_Transformation_sample {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    /*
      -withReplacement  是否抽样放回
        true    抽样放回
        false   抽样不放回
      -fraction
        withReplacement=true   表示期望每一个元素出现的次数  >0
        withReplacement=false  表示RDD中每一个元素出现的概率[0,1]
      -seed 抽样算法初始值
        一般不需要指定
     */
    // 从rdd中随机抽取一些数据(抽样放回)
    // val newRDD: RDD[Int] = rdd.sample(true, 3)
    // newRDD.collect.foreach(println)
    // 从rdd中随机抽取一些数据(抽样不放回)
    // val newRDD: RDD[Int] = rdd.sample(false, 0.6,3)
    // newRDD.collect.foreach(println)

    val stds: List[String] = List("李四","王五","jio jio","丽丽","小红","张三","空空")
    val nameRDD: RDD[String] = sc.makeRDD(stds)
    //从上面RDD中抽取一名幸运同学进行连麦
    val luckyMan: Array[String] = nameRDD.takeSample(false,1)
    luckyMan.foreach(println)
    // 关闭连接
    sc.stop()
  }
}
