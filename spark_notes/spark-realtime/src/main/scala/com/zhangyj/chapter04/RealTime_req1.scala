package com.zhangyj.chapter04

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.{Decoder, StringDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag

/**
  * Desc: 需求：每天每地区热门广告top3
  */
object RealTime_req1 {
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

    // 测试从Kafka中消费数据，
    //从kafka的kv值中取value     msg = 1590136353874,华北,北京,103,1
    val dataDS: DStream[String] = kafkaDS.map(_._2)
    // 打印输出
    // dataDS.print()

    //==========需求一实现： 每天每地区热门广告   msg = 1584271384370,华南,广州,100,1==========

    //1.对获取到的Kafka中传递的原始数据
    //2.将原始数据转换结构  (天_地区_广告,点击次数)
    //将从kafka获取到的原始数据进行转换  ==>(天_地区_广告,1)
    val mapDS: DStream[(String, Int)] = dataDS.map(s => {
      val fields = s.split(",")
      //格式化时间戳
      val timeStamp: Long = fields(0).toLong
      val date: Date = new Date(timeStamp)
      val dfs = new SimpleDateFormat("yyyy-MM-DD")
      val dayStr = dfs.format(date)
      val area = fields(1)
      val adv = fields(4)

      ((dayStr + "_" + area + "_" + adv), 1)
    })

    //3.对每天每地区广告点击数进行聚合处理 (天_地区_广告,点击次数sum)
    //注意：这里要统计的是一天的数据，所以要将每一个采集周期的数据都统计，需要传递状态，所以要用udpateStateByKey
    val updateDS: DStream[(String, Int)] = mapDS.updateStateByKey((seq: Seq[Int], buffer: Option[Int]) => {
      Option(seq.sum + buffer.getOrElse(0))
    })

    //4.将聚合后的数据进行结构的转换   (天_地区,(广告,点击次数sum)))
    val mapDS1: DStream[(String, (String, Int))] = updateDS.map {
      case (k, sum) => {
        val ks = k.split("_")
        (ks(0) + "_" + ks(1), (ks(2), sum))
      }
    }
    //5.按照天_地区对数据进行分组  (时间,Iterator[(地区,(广告,点击次数sum))])
    val groupDS: DStream[(String, Iterable[(String, Int)])] = mapDS1.groupByKey()
    //6.对分组后的数据降序取前三
    val resDS: DStream[(String, List[(String, Int)])] = groupDS.mapValues {
      datas => {
        // sortBy默认是递增，使用负值
        datas.toList.sortBy(-_._2).take(3)
      }
    }
    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
