package com.zhangyj.chapter05

/**
 * Date: 2020/4/28
 * Desc: 高阶函数之函数可以作为参数进行传递  ---  匿名函数
 *   -扩展函数的功能
 *   -提高函数的灵活度
 */
object Scala07_TestFun_Non {
  def main(args: Array[String]): Unit = {
    //需求：提供一个函数，对数组中的元素进行处理，处理完毕之后返回一个新的数组   目前处理方式：元素 + 1
    //创建一个数组对象 val arr: Array[Int] = Array(1,2,3,4)

    def operationArray(arr:Array[Int],op:Int=>Int): Array[Int] ={
      //遍历数组中的元素
      for (elem<-arr) yield op(elem)
    }

    //def op(elem:Int):Int={
    //  elem +1
    //}
    //调用函数，实现+1功能
    //val newArr: Array[Int] = operationArray(Array(1,2,3,4),op)

    //打印输出，  mkString:用指定的字符串连接数组中的元素，形成一个新的字符串
    //println(newArr.mkString(","))

    //通过匿名函数调用，对数组元素 * 3

    //operationArray(Array(1,2,3,4),(elem:Int)=>{elem *3 })


    operationArray(Array(1,2,3,4),_ * 3 )

  }
}
