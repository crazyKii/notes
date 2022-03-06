package com.spark.chapter04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用netcat工具向9999端口不断的发送数据，通过SparkStreaming读取端口数据并统计不同单词出现的次数
 */
object SparkStreaming01_wordcount {

  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从指定的端口获取数据
    val socketDs: ReceiverInputDStream[String] = ssc.socketTextStream("node101", 9999)
    //扁平化
    val flatMapDs: DStream[String] = socketDs.flatMap(_.split(" "))
    //结构转换  进行计数
    val mapDs: DStream[(String, Int)] = flatMapDs.map((_, 1))
    //聚合
    val reduceDs: DStream[(String, Int)] = mapDs.reduceByKey((_ + _))
    //打印输出
    reduceDs.print
    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()

    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }

}
