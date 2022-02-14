package com.spark.chapter02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Spark01_TopN_req2 {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //1.读取数据，创建RDD
    val dataRDD: RDD[String] = sc.textFile("/Users/zhangyongjie/Documents/IdeaProjects/notes_code/spark_notes/data/user_visit_action.txt")

    //2.将读到的数据进行切分，并且将切分的内容封装为UserVisitAction对象
    val actionRDD: RDD[UserVisitAction] = dataRDD.map(line => {
      val fields: Array[String] = line.split("_")
      UserVisitAction(
        fields(0),
        fields(1).toLong,
        fields(2),
        fields(3).toLong,
        fields(4),
        fields(5),
        fields(6).toLong,
        fields(7).toLong,
        fields(8),
        fields(9),
        fields(10),
        fields(11),
        fields(12).toLong
      )
    })

    //3.判断当前这条日志记录的是什么行为，并且封装为结果对象    (品类,点击数,下单数,支付数)==>如:CategoryCountInfo(鞋,1,1,1)
    // 这里下单和支付操作可能会有多个品类，所以我们这里使用扁平化进行分解
    // 点击、下单、支付的动作获取品类的方式是不一样的，所以需要判断不同的行为
    // 1)如果搜索关键字是null，表示这次不是搜索
    // 2)如果点击的品类id和产品id是-1表示这次不是点击
    // 3)下单行为来说一次可以下单多个产品，所以品类id和产品id都是多个，id之间使用逗号，分割。如果本次不是下单行为，则他们相关数据用null来表示
    // 4)支付与下单一致
    // 数据过程:
    //(鞋,1,0,0)
    //(保健品,1,0,0)
    //(鞋,0,1,0)
    //(保健品,0,1,0)
    //(鞋,0,0,1)=====>(鞋,1,1,1)
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(userAction => {
      if (userAction.click_category_id != "-1") {
        // 封装，点击为1，其他为0
        List(CategoryCountInfo(userAction.click_category_id.toString, 1, 0, 0))
      } else if (userAction.order_category_ids != "null") {
        //坑：读取的文件应该是null字符串，而不是null对象
        //判断是否为下单行为,如果是下单行为，需要对当前订单中涉及的所有品类Id进行切分
        val ids: Array[String] = userAction.order_category_ids.split(",")
        //定义一个集合，用于存放多个品类id封装的输出结果对象
        val categoryCountInfoList: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
        // 对所有品类的id进行遍历
        for (id <- ids) {
          // 封装，订单为1
          categoryCountInfoList.append(CategoryCountInfo(id, 0, 1, 0))
        }
        categoryCountInfoList
      } else if (userAction.pay_category_ids != "null") {
        // 支付
        val ids: Array[String] = userAction.pay_category_ids.split(",")
        //定义一个集合，用于存放多个品类id封装的输出结果对象
        val categoryCountInfoList: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
        // 对所有品类的id进行遍历
        for (id <- ids) {
          // 封装，支付为1
          categoryCountInfoList.append(CategoryCountInfo(id, 0, 0, 1))
        }
        categoryCountInfoList
      } else {
        Nil
      }
    })

    //4.将相同品类的分成一组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(info => info.categoryId)

    //5.将分组后的数据进行聚合处理: 返回一个元组(品类id, 聚合后的CategoryCountInfo)
    val reduceRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues(datas => {
      datas.reduce {
        (info1, info2) => {
          info1.clickCount = info1.clickCount + info2.clickCount
          info1.orderCount = info1.orderCount + info2.orderCount
          info1.payCount = info1.payCount + info2.payCount
          info1
        }
      }
    })

    //6.对上述RDD的结构进行转换，只保留value部分,得到聚合之后的RDD[CategoryCountInfo]
    val mapRDD: RDD[CategoryCountInfo] = reduceRDD.map(_._2)

    //7.对RDD中的数据排序，取前10
    val res: Array[CategoryCountInfo] = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)

    //8.打印输出
    //res.foreach(println)

    //===============================需求二===================================
    //1.获取热门品类top10的品类id
    val ids: Array[String] = res.map(_.categoryId)

    //ids可以进行优化， 因为发送给Excutor中的Task使用，每一个Task都会创建一个副本，所以可以使用广播变量
    val broadcastIds: Broadcast[Array[String]] = sc.broadcast(ids)
    //2.将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      if (action.click_category_id != "-1") {
        //同时确定是热门品类的点击
        //坑   集合数据为字符串类型，id是Long类型，需要进行转换
        broadcastIds.value.contains(action.click_category_id.toString)
      } else {
        false
      }
    })

    //3.对session的点击数进行转换 (category_session,1)
    val mapRDD1: RDD[(String, Int)] = filterRDD.map {
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    }

    //4.对session的点击数进行统计 (category_session,sum)
    val reduceRDD1: RDD[(String, Int)] = mapRDD1.reduceByKey(_ + _)

    //5.将统计聚合的结果进行转换  (category,(session,sum))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD1.map {
      case (categoryAndSession, sum) => {
        (categoryAndSession.split("_")(0), (categoryAndSession.split("_")(1), sum))
      }
    }
    //6.将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    //7.对分组后的数据降序 取前10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(10)
      }
    }
    resRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
