package com.zhangyj.chapter05

/**
 * Date: 2020/4/28
 * Desc: 通过递归求一个整数的阶乘
 */
object Scala09_TestFun_jc {

  def main(args: Array[String]): Unit = {
    println(jc(5))
  }

  def jc(n:Int):Int={
    if(n==1){
      return 1
    }
    n * jc(n-1)
  }

}

