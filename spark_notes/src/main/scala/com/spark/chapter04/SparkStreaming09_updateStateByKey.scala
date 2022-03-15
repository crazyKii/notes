package com.spark.chapter04

import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming_Test").setMaster("local[*]")
    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点路径   状态保存在checkpoint中
    ssc.checkpoint("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/cp")
    // 从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("node101", 9999)
    // 扁平化
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))
    // 结构转化
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))
    //聚合   reduceByKey是无状态的，只会对当前采集周期的数据进行聚合操作
    //val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    //reduceDS.print

    /*
    (hello,1),(hello,1),(hello,1)===>hello----->(1,1,1)
    * */
    val stateDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      //第一个参数：表示的相同的key对应的value组成的数据集合
      //第二个参数：表示的相同的key的缓冲区数据
      (seq: Seq[Int], state: Option[Int]) => {
        //对当前key对应的value进行求和
        //seq.sum
        //获取缓冲区数据
        //state.getOrElse(0)
        Option(seq.sum + state.getOrElse(0))
      }
    )
    stateDS.print()


    ssc.start()

    ssc.awaitTermination()
  }
}
