package com.spark.chapter04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Desc:常用输出操作
 */
object SparkStreaming11_output {

  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("node101", 9999)

    //设置窗口的大小以及滑动步长   以上两个值都应该是采集周期的整数倍
    val windowDS: DStream[String] = socketDS.window(Seconds(6), Seconds(3))
    val resDS: DStream[(String, Int)] = windowDS.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // resDS.print()

    //输出数据到指定的文件
    // resDS.saveAsTextFiles("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/output/wordcount","part")

    resDS.foreachRDD(rdd => {
      //将RDD中的数据保存到MySQL数据库
      rdd.foreachPartition{
        //注册驱动
        //获取连接
        //创建数据库操作对象
        //执行SQL语句
        //处理结果集
        //释放资源
        datas=>{
          //....
        }
      }
    })

    //启动采集器
    ssc.start()

    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
