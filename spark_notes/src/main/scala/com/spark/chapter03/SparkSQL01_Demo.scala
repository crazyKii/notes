package com.spark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:   演示RDD&DF&DS之间关系以及转换
 */
object SparkSQL01_Demo {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")

    //创建SparkSQL执行的入口点对象  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    /*
    // 读取json文件创建DataFrame
    val df: DataFrame = spark.read.json("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/spark_sql_data/people.json")
    //查看df里面的数据
    df.show()

    // SQL风格语法
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()

    // DSL风格
    df.select("name","age").show()
    */

    //RDD--->DataFrame--->DataSet
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "banzhang", 20), (2, "jingjing", 18), (3, "wangqiang", 30)))
    //RDD--->DataFrame
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //DataFrame--->DataSet
    val ds: Dataset[User] = df.as[User]

    //DataSet--->DataFrame--->RDD
    val df2: DataFrame = ds.toDF()
    val rdd1: RDD[Row] = df2.rdd
    rdd1.collect().foreach(print)
    // 关闭连接
    spark.stop()
  }

}
case class User(id:Int,name:String,age:Int)
