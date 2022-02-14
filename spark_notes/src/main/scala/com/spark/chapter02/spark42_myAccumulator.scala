package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 累加器
 *   自定义累加器,统计出RDD中，所有以"H"开头的单词以及出现次数(word,count)
 */
object spark42_userAccumulator {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    // 1.创建累加器对象
    val myAcc: MyAccumulator = new MyAccumulator
    // 2. 注册累加器
    sc.register(myAcc)
    // 3. 使用累加器
    rdd.foreach(
      word => {
        myAcc.add(word)
      }
    )

    //输出累加器结果
    println(myAcc.value)

    // 关闭连接
    sc.stop()
  }
}

import scala.collection.mutable
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]] {
  // 定义一个集合，集合单词以及出现次数
  var map = mutable.Map[String,Int]()

  // 是否是初始状态
  override def isZero: Boolean = map.isEmpty

  // 拷贝
  override def copy(): AccumulatorV2[String,mutable.Map[String,Int]] = {
    val newAcc: MyAccumulator = new MyAccumulator
    newAcc.map = this.map
    newAcc
  }

  // 重置
  override def reset(): Unit = map.clear()

  // 向累加器中添加元素
  override def add(elem: String): Unit = {
    if(elem.startsWith("H")){
      map(elem) = map.getOrElse(elem,0) + 1
    }
  }

  // 合并excutor中的数据
  override def merge(other: AccumulatorV2[String,mutable.Map[String,Int]]): Unit = {
    // 当前Executor的值
    var map1: mutable.Map[String, Int]  = map
    // 另一个Executor的map
    var map2: mutable.Map[String, Int] = other.value

    map = map1.foldLeft(map2) {
      // mm表示map2,kv表示map1中的每一个元素
      (mm,kv) => {
        // 指定合并规则
        val k: String = kv._1
        val v: Int = kv._2
        // 根据map1中元素的key，到map2中获取value
        mm(k) = mm.getOrElse(k,0) + v
        mm
      }
    }
  }

  // 获取累加器的值
  override def value: mutable.Map[String,Int] = map
}
