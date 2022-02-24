package com.spark.chapter03

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Desc: 通过jdbc对MySQL进行读写操作
 */
object SparkSQL07_DataSource_Mysql_read {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL07_DataSource_Mysql")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    /*
    //方式1：通用的load方法读取
    // 相关配置，从option找到 JDBCOptions 查看
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.82.101:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()
    df.show()
    */

    /*
    //方式2:通用的load方法读取 参数另一种形式
    val df = spark.read.format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://192.168.82.101:3306/test?user=root&password=123456",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "user"
        )
      )
      .load()
    df.show()
    */

    //方式3:使用jdbc方法读取
    val props: Properties = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    props.setProperty("driver","com.mysql.jdbc.Driver")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.82.101:3306/test","user",props)
    df.show()

    //释放资源
    spark.stop()
  }
}
