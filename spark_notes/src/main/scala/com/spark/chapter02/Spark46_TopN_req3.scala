package com.spark.chapter02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Spark01_TopN_req3 {

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

    //========================================需求三 ========================================
    val pageIdRDD: RDD[(Long, Long)] = actionRDD.map {
      action => {
        (action.page_id, 1L)
      }
    }

    val fmIdsMap: Map[Long, Long] = pageIdRDD.reduceByKey(_ + _).collect().toMap

    /*
      zs  11:35:00  首页
      ls  11:35:00  首页
      zs  11:36:00  详情
      zs  11:37:00  下单
    */
    //3.计算分子
    //3.1 将原始数据根据sessionId进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    //3.2 将分组后的数据按照时间进行升序排序
    val pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues {
      datas => {
        //得到排序后的同一个session的用户访问行为
        val userActions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }
        //3.3 对排序后的用户访问行为进行结构转换，只保留页面就可以
        val pageIdsList: List[Long] = userActions.map(_.page_id)
        //A->B->C->D->E->F
        //B->C->D->E->F
        //3.4 对当前会话用户访问页面 进行拉链  ,得到页面的流转情况 (页面A,页面B)
        val pageFlows: List[(Long, Long)] = pageIdsList.zip(pageIdsList.tail)
        //3.5 对拉链后的数据，进行结构的转换 (页面A-页面B,1)
        pageFlows.map {
          case (pageId1, pageId2) => {
            (pageId1 + "-" + pageId2, 1)
          }
        }

      }
    }
    // 3.6 将每一个会话的页面跳转统计完毕之后，没有必要保留会话信息了，所以对上述RDD的结构进行转换
    //只保留页面跳转以及计数
    val pageFlowMapRDD: RDD[(String, Int)] = pageFlowRDD.map(_._2).flatMap(list=>list)

    //3.7 对页面跳转情况进行聚合操作
    val pageAToPageBSumRDD: RDD[(String, Int)] = pageFlowMapRDD.reduceByKey(_+_)

    //4.页面单跳转换率计算
    pageAToPageBSumRDD.foreach{
      //(pageA-pageB,sum)
      case (pageFlow,fz)=>{
        val pageIds: Array[String] = pageFlow.split("-")
        //获取分母页面id
        val fmPageId: Long = pageIds(0).toLong
        //根据分母页面id，获取分母页面总访问数
        val fmSum: Long = fmIdsMap.getOrElse(fmPageId,1L)
        //转换率
        println(pageFlow +"--->" + fz.toDouble / fmSum)
      }
    }

    // 关闭连接
    sc.stop()
  }
}
