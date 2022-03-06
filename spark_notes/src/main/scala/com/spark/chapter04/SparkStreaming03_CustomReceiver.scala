package com.spark.chapter04

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Desc: 通过自定义数据源方式创建DStream
 *      模拟从指定的网络端口获取数据
 */
object SparkStreaming03_CustomReceiver {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming_DStream").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 通过自定义数据源创建Dstream
    val myDs: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("node101", 9999))
    //扁平化
    val flatMapDs: DStream[String] = myDs.flatMap(_.split(" "))
    //结构转换  进行计数
    val mapDs: DStream[(String, Int)] = flatMapDs.map((_, 1))
    //聚合
    val reduceDs: DStream[(String, Int)] = mapDs.reduceByKey((_ + _))
    //打印输出
    reduceDs.print
    //启动采集器
    ssc.start()

    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }

}

//Receiver[T]  泛型表示的是 读取的数据类型
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //创建一个Socket
  private var socket: Socket = _

  //读数据并将数据发送给Spark
  def receive(): Unit ={
    //创建连接
    socket = new Socket(host,port)

    try{
      //创建一个BufferedReader用于读取端口传来的数据
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      //定义一个变量，用来接收端口传过来的数据
      var input:String = null
      //读取数据 循环发送数据给Spark 一般要想结束发送特定的数据 如："==END=="
      while((input = reader.readLine()) != null){
        store(input)
      }
    }catch {
      case e:ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }finally {
      onStop()
    }
  }

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {

    new Thread("Socket Receiver"){
      setDaemon(true)
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {
    synchronized{
      if(socket != null){
        socket.close()
        socket = null
      }
    }
  }
}
