package com.zhangyj.chapter04

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 需求二：统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）
 *   -采集周期： 3s
 *   1.最近一个小时    窗口的长度为1小时
 *   2.每6s更新一次    窗口的滑动步长是6s
 *   3.各个广告每分钟的点击量 ((advId,hhmm),1)
 */
object RealTime_req2 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")

    //创建SparkStreaming执行的上下文
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.sparkContext.setCheckpointDir("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/cp")

    //kafka参数声明
    val brokers = "node101:9092,node102:9092,node103:9092"
    val topic = "my-ads"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建DS
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("my-ads"))

    //测试Kafka中消费数据   msg = 1584271384370,华南,广州,100,1
    val dataDS: DStream[String] = kafkaDS.map(_._2)

    //定义窗口
    //这里为了测试，将窗口大小改为12s，触发计算时长改为3s（即，与采集周期相同，采集完就计算）
    val windowDS: DStream[String] = dataDS.window(Seconds(12), Seconds(3))

    //转换结构为 ((advId,hhmm),1)
    val mapDS = windowDS.map(x => {
      val fields: Array[String] = x.split(",")
      val timeStamp: Long = fields(0).toLong
      val day = new Date(timeStamp)
      val sdf = new SimpleDateFormat("hh:mm")
      val time = sdf.format(day)
      ((fields(4), time), 1)
    })

    //对数据进行聚合
    val resDS: DStream[((String, String), Int)] = mapDS.reduceByKey(_ + _)

    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
