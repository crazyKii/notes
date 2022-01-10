package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过集合创建rdd
 *   取决于分配给应用的CPU核数
 * 通过读取外部文件创建rdd
 *   math.min(取决于分配给应用的CPU的核数，2)
 */
object Spark03_Partition_default {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    // 1、通过集合创建rdd
    // val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 查看分区效果
    // println(rdd.partitions.size) // 12
    // 默认产生12个文件
    // rdd.saveAsTextFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/output")

    // 2、通过读取外部文件创建rdd
    val rdd: RDD[String] = sc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/input")
    print(rdd.partitions.size) // 2个
    // 默认产生2个文件
    rdd.saveAsTextFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/output")
    // 关闭
    sc.stop()
  }
}
