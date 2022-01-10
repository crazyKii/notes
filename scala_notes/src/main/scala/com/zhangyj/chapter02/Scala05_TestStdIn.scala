package com.zhangyj.chapter02

import scala.io.StdIn

/**
  * Desc: 获取用户键盘上的输入
  */
object Scala05_TestStdIn {
  def main(args: Array[String]): Unit = {
    // 1 输入姓名
    println("input name:")
    var name = StdIn.readLine()

    // 2 输入年龄
    println("input age:")
    var age = StdIn.readShort()

    // 3 输入薪水
    println("input sal:")
    var sal = StdIn.readDouble()

    // 4 打印
    println(s"name= ${name} ,age=${age},sal=${sal}")
  }
}
