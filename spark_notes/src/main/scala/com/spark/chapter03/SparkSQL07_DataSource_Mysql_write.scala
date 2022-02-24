package com.spark.chapter03

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * Desc: 通过jdbc对MySQL进行读写操作
 */
object SparkSQL07_DataSource_Mysql_write {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL07_DataSource_Mysql")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2(20,"lisi"), User2(30,"zs")))
    //将RDD转换为DF
    //val df: DataFrame = rdd.toDF()
    //df.write.format("jdbc").save()

    //将RDD转换为DS
    val ds: Dataset[User2] = rdd.toDS()
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://192.168.82.101:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option(JDBCOptions.JDBC_TABLE_NAME,"user")
      .mode(SaveMode.Append)
      .save()

    //释放资源
    spark.stop()
  }
}

case class User2(age: Int,name: String)
