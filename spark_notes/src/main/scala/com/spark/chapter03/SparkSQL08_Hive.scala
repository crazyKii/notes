package com.spark.chapter03

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL08_Hive {
  def main(args: Array[String]): Unit = {
    // 创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Hive")
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    spark.sql("show databases").show()

    // 释放资源
    spark.stop()
  }
}
