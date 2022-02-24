package com.spark.chapter03

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}


/**
 * Desc: 自定义UDAF（强类型  主要应用在DSL风格的DS查询）
 */
object SparkSQL06_UDAF2 {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlTest").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/spark_sql_data/people.json")

    /*
     注意：如果是自定义UDAF的强类型，没有办法应用SQL风格DF的查询
     //注册自定义函数
     spark.udf.register("myAvgNew",myAvgNew)
     //创建临时视图
     df.createOrReplaceTempView("user")
     //使用聚合函数进行查询
     spark.sql("select myAvgNew(age) from user").show()
    */

    import spark.implicits._
    //创建自定义函数对象
    val myAvgNew = new MyAvgNew
    //将df转换为ds
    val ds: Dataset[People] = df.as[People]
    //将自定义函数对象转换为查询列
    val col: TypedColumn[People, Double] = myAvgNew.toColumn
    //在进行查询的时候，会将查询出来的记录（User06类型）交给自定义的函数进行处理
    ds.select(col).show

    spark.close()
  }
}

//输入类型的样例类
case class People (name:String, age:Long)

//缓存类型
case class AgeBuffer(var ageSum:Long, var ageCount:Long)

//自定义UDAF函数(强类型)
//* @tparam IN 输入数据类型
//* @tparam BUF 缓存数据类型
//* @tparam OUT 输出结果数据类型
class MyAvgNew extends Aggregator[People,AgeBuffer,Double]{
  //对缓存数据进行初始化
  override def zero: AgeBuffer = {
    AgeBuffer(0L,0L)
  }

  //对当前分区内数据进行聚合
  override def reduce(b: AgeBuffer, a: People): AgeBuffer = {
    b.ageSum += a.age
    b.ageCount += 1
    b
  }

  //分区间合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.ageSum = b1.ageSum + b2.ageSum
    b1.ageCount = b1.ageCount + b2.ageCount
    b1
  }

  //返回计算结果
  override def finish(reduction: AgeBuffer): Double = {
    reduction.ageSum.toDouble / reduction.ageCount
  }

  //DataSet的编码以及解码器  ，用于进行序列化，固定写法
  //用户自定义Ref类型  product       系统值类型，根据具体类型进行选择
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}




