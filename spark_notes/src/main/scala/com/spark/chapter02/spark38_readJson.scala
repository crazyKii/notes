package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * Desc: 读取Json格式数据
 */
object spark38_readJson {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val scc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = scc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/input/test.json")

    val resRDD: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    resRDD.collect().foreach(println)
    // 关闭连接
    scc.stop()
  }
}
