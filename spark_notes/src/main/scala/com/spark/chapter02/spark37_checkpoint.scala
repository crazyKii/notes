package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: RDD的检查点
 */
object spark37_checkpoint {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 设置检查点目录
    sc.setCheckpointDir("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/cp")
    //开发环境，应该将检查点目录设置在hdfs上
    // 设置访问HDFS集群的用户名
    //System.setProperty("HADOOP_USER_NAME","atguigu")
    //sc.setCheckpointDir("hdfs://hadoop202:8020/cp")

    //3. 创建一个RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    //3.1.业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    //打印血缘关系
    println(wordToOneRdd.toDebugString)

    //设置检查点
    wordToOneRdd.checkpoint()

    //在开发环境，一般检查点和缓存配合使用
    wordToOneRdd.cache()

    //3.2 触发执行逻辑
    wordToOneRdd.collect().foreach(println)

    println("---------------------------------------")

    //打印血缘关系
    println(wordToOneRdd.toDebugString)

    //释放缓存
    //wordToOneRdd.unpersist()

    //触发行动操作
    wordToOneRdd.collect().foreach(println)

    Thread.sleep(10000000)
    // 关闭连接
    sc.stop()
  }
}
