package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求说明：WordCount
 */
object Spark12_WordCount {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    //简单版-实现方式1
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    // 对RDD中的元素进行扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    // 将RDD中的单词进行分组
    val groupByRDD: RDD[(String, Iterable[String])] = flatMapRDD.groupBy(item => item)
    // 对分组之后的数据进行映射
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }
    resRDD.collect().foreach(println)
    */

    /*
    // 复杂版 方式1
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    // 将原RDD中字符串以及字符串出现的次数，进行处理，形成一个新的字符串
    //val mapRDD: RDD[String] = rdd.map(datas => (datas._1 + " ") * datas._2)
    val mapRDD: RDD[String] = rdd.map {
      case (str, count) => {
        (str + " ") * count
      }
    }
    val flatMapRDD: RDD[String] = mapRDD.flatMap(_.split(" "))
    val groupByRDD: RDD[(String, Iterable[String])] = flatMapRDD.groupBy(item => item)
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }
    resRDD.collect().foreach(println)
    */

    //复杂版  方式2
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    val flatMapRDD: RDD[(String, Int)] = rdd.flatMap {
      case (line, count) => {
        line.split(" ").map(word => (word, count))
      }
    }

    // 按照单词对RDD中的元素进行分组     (Hello,CompactBuffer((Hello,2), (Hello,3), (Hello,2)))
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = flatMapRDD.groupBy(_._1)

    // 对RDD的元素重新进行映射
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }
    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
