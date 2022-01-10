package com.zhangyj.chapter02

import scala.collection.immutable.StringOps
import scala.runtime.BoxedUnit

/**
  * Desc: 数据类型
  */
object Scala06_DataType {
  def main(args: Array[String]): Unit = {
    //整数类型
    // 正确
    //var n1:Byte = 127
    //var n2:Byte = -128

    // 错误，超过数据范围
    // var n3:Byte = 128
    // var n4:Byte = -129

    //在编译的时候，会判断当前数值是否在当前类型表示范围之内，如果在，底层默认进行转换
    //var b:Byte = 30
    var b:Byte = 10 + 20
    // var b1:Byte = 10
    // 报错 byte变量 + int 自动转化为int
    // var b:Byte = b1 + 20
    println(b)

   /* //字符类型
    //（1）字符常量是用单引号 ' ' 括起来的单个字符。
    var c1: Char = 'a'
    println("c1=" + c1)
    //注意：这里涉及自动类型提升，其实编译器可以自定判断是否超出范围，
    //不过idea提示报错
    var c2:Char = 'a' + 1
    println(c2)

    //（2）\t ：一个制表位，实现对齐的功能
    println("姓名\t年龄")

    //（3）\n ：换行符
    println("西门庆\n潘金莲")

    //（4）\\ ：表示\
    println("c:\\岛国\\avi")

    //（5）\" ：表示"
    println("同学们都说：\"大海哥最帅\"")
*/


    //println(m1())

    //println(m2(1))
  }

  //演示Unit
  def m1(): Unit ={
    println("m1方法被执行了")
  }

  //演示Nothing
  def m2(n:Int): Nothing ={
    if(n==0){
      throw new NullPointerException
    }else{
      //在Scala语言中，Nothing类型表示发生异常了
      throw new RuntimeException
    }
  }

}
