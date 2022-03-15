package com.spark.chapter04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc:  有状态的转换---window
 */
object SparkStreaming10_window {

  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("node101", 9999)

    /*
    //设置窗口的大小以及滑动步长   以上两个值都应该是采集周期的整数倍
    val windowDS: DStream[String] = socketDS.window(Seconds(6), Seconds(3))
    val resDS: DStream[(String, Int)] = windowDS.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
     */

    val resDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => {
          x + y
        },
        Seconds(6), Seconds(3)
      )

    resDS.print()

    //启动采集器
    ssc.start()

    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }

}
