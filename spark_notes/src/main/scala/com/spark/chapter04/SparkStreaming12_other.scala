package com.spark.chapter04

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Desc：DStream编程进阶
 */
object SparkStreaming12_other {
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

    //在DStream中使用累加器，广播变量以及缓存
    //ssc.sparkContext.longAccumulator
    //ssc.sparkContext.broadcast(10)

    //缓存
    //resDS.cache()
    //resDS.persist(StorageLevel.MEMORY_ONLY)

    //检查点
    //ssc.checkpoint("")

    //使用SparkSQL处理采集周期中的数据
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    resDS.foreachRDD(
      rdd=>{
        rdd
        //将RDD转换为DataFrame
        val df: DataFrame = rdd.toDF("word","count")
        //创建一个临时视图
        df.createOrReplaceTempView("words")
        //执行SQL
        spark.sql("select * from words").show
      }
    )

    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()

    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
