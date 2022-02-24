package com.spark.chapter03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

/**
 * Desc: 求平均年龄----通过累加器实现
 */
object SparkSQL04_UDAF_accumulator {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",20),("bangzhang",40),("xuchong",30)))

    //创建累加器对象
    val myAcc: MyACC = new MyACC

    // 注册累加器
    sc.register(myAcc)

    //使用累加器
    rdd.foreach{
      case (name,age) =>{
        myAcc.add(age)
      }
    }

    // 获取累加器的值
    println(myAcc.value)

    // 关闭连接
    sc.stop()

  }
}

class MyACC extends AccumulatorV2[Int,Int]{
  var sum:Int = 0
  var count:Int = 0
  override def isZero: Boolean = {
    return sum ==0 && count == 0
  }

  override def copy(): AccumulatorV2[Int, Int] = {
    val newMyAc = new MyACC
    newMyAc.sum = this.sum
    newMyAc.count = this.count
    newMyAc
  }

  override def reset(): Unit = {
    sum =0
    count = 0
  }

  override def add(v: Int): Unit = {
    sum += v
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o:MyACC=>{
        sum += o.sum
        count += o.count
      }
      case _=>
    }

  }

  override def value: Int = sum/count
}