package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 转换算子-reduceByKey
 *   -根据相同的key对RDD中的元素进行聚合
 */
object Spark22_Transformation_groupByKey {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    /* groupBy与groupByKey区别

    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    //  (a,CompactBuffer((a,1), (a,5)))
    groupByRDD.collect().foreach(println)


    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    // (a,CompactBuffer(1, 5))
    groupByKeyRDD.collect().foreach(println)
    */
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    // 分组求和
    val newRDD: RDD[(String, Int)] = groupByKeyRDD.map {
      case (key, datas) => {
        (key, datas.sum)
      }
    }
    newRDD.collect().foreach(println)

    //Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
