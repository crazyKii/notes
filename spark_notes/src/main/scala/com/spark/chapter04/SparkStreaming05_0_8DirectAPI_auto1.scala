package com.spark.chapter04

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Desc:   通过DirectAPI连接Kafka数据源，获取数据
 *   自定的维护偏移量，偏移量维护在checkpiont中
 *   目前我们这个版本，只是指定的检查点，只会将offset放到检查点中，但是并没有从检查点中取，会存在消息丢失
 */
object SparkStreaming05_0_8DirectAPI_auto1 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点目录
    ssc.checkpoint("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/cp")

    //准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node101:9092,node102:9092,node103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test_spark"
    )

    //连接Kafka，创建DStream
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("dstream-kafka-08")
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
