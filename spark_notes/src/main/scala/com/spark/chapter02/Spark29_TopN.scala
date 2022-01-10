package com.spark.chapter02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:  需求：统计每一个省份广告点击排名前3的
 */
object Spark29_TopN {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //1.读取外部文件，创建RDD    时间戳 省份id 城市id 用户id 广告id
    val logRDD: RDD[String] = sc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/input/agent.log")

    //2.对读取到的数据，进行结构转换  (省份id-广告id,1)
    val mapRDD: RDD[(String, Int)] = logRDD.map(line => {
      //2.1  用空格对读取的一行字符串进行切分
      val fields: Array[String] = line.split(" ")
      //2.2 封装为元组结构返回
      (fields(1) + "-" + fields(4), 1)
    })

    //3.对当前省份的每一个广告点击次数进行聚合  (省份A-广告A,1000)  (省份A-广告B,800)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //4.再次对结构进行转换，将省份作为key   (省份A,(广告A,1000))  (省份A,(广告B,800))
    val map1Rdd: RDD[(String, (String, Int))] = reduceRDD.map {
      case (proAndAd, clickCount) => {
        val proAndAdArr: Array[String] = proAndAd.split("-")
        (proAndAdArr(0), (proAndAdArr(1), clickCount))
      }
    }

    //5.按照省份对数据进行分组    (省份, Iterable[(广告A, 80),(广告B, 100),(广告C, 90),(广告D, 200)....])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1Rdd.groupByKey()

    //6.对每一个省份中的广告点击次数进行降序排序并取前3名
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(itr => {
      itr.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })
    resRDD.collect().foreach(println)

    //Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
