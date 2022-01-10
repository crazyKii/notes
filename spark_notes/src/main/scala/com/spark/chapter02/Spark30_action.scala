package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:  行动算子
 */
object Spark30_action {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // reduce 聚合
    val res: Int = rdd.reduce(_ + _)
    println(res)
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // collect 以数组的形式返回数据集
    val ints: Array[Int] = rdd.collect()
    ints.foreach(println)
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // count 获取RDD中元素的个数
    val count: Long = rdd.count()
    println(count)
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // first 返回RDD中的第一个元素
    val res: Int = rdd.first()
    println(res)
     */
    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // take(n) 返回由RDD前n个元素组成的数组
    val ints: Array[Int] = rdd.take(2)
    println(ints.mkString(","))
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    // takeOrdered(n) 返回该RDD排序后前n个元素组成的数组
    val ints: Array[Int] = rdd.takeOrdered(2)
    println(ints.mkString(","))
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),8)
    // aggregate
    //val res: Int = rdd.aggregate(0)(_ + _, _ + _) //10
    val res: Int = rdd.aggregate(10)(_ + _, _ + _) // 100
    print(res)
    */

    /*
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),8)
    // fold
    val res: Int = rdd.fold(10)( _ + _) // 100
    print(res)
    */

    /*
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b")))
    //countByKey  统计每种key出现的次数
    val res: collection.Map[Int, Long] = rdd.countByKey()
    println(res)
    */

    /*
    //save相关的算子
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //保存为文本文件
    //rdd.saveAsTextFile("/Users/zhangyongjie/Documents/output")
    //保存序列化文件
    //rdd.saveAsObjectFile("/Users/zhangyongjie/Documents/output")
    //保存为SequenceFile   注意：只支持kv类型RDD
    rdd.map((_,1)).saveAsSequenceFile("/Users/zhangyongjie/Documents/output")
    */

    // foreach(f)遍历RDD中每一个元素
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    rdd.foreach(println)


//Thread.sleep(1000000)
// 关闭连接
sc.stop()
}
}
