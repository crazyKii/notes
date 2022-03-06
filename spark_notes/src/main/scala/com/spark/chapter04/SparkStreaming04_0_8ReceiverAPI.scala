package com.spark.chapter04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Desc:   通过ReceiverAPI连接Kafka数据源，获取数据
 */
object SparkStreaming04_0_8ReceiverAPI {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming02_RDDQueue").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //连接Kafka，创建DStream
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "node101:2181,node102:2181,node103:2181/kafka_2.4",
      "test_spark",
      Map("dstream-kafka-08" -> 3)
    )
    //获取kafka中的消息
    val lineDS: DStream[String] = kafkaDstream.map(_._2)

    //扁平化
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    reduceDS.print

    //开启任务
    ssc.start()

    ssc.awaitTermination()
  }
}
