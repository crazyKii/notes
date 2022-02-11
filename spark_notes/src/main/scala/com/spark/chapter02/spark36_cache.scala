package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 *Desc: RDD的缓存
 */
object spark36_cache {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 1、创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    // 1.1 扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    // 1.2 结构转换
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map {
      word => {
        println("********************")
        (word, 1)
      }
    }
    // 2、打印血缘关系
    print( mapRDD.toDebugString)
    // 3.1 对RDD的数据进行缓存   底层调用的是persist函数   默认缓存在内存中
    //     cache操作会增加血缘关系，不改变原有的血缘关系
    // mapRDD.cache()

    // 3.2 persist可以接收参数，指定缓存位置
    // 注意：虽然叫持久化，但是当应用程序程序执行结束之后，缓存的目录也会被删除
    // mapRDD.persist()
    // 3.3 可以更改存储级别
    mapRDD.persist(StorageLevel.DISK_ONLY)

    // 4 触发行动操作
    mapRDD.collect()

    println("---------缓存后重新执行算子------------------------------")
    // 打印血缘关系
    println(mapRDD.toDebugString)
    // 5 再次触发行动操作
    mapRDD.collect()

    // 关闭连接
    sc.stop()
  }
}
