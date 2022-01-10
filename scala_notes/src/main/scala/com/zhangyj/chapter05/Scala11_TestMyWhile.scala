package com.zhangyj.chapter05
/**
 * Date: 2020/4/28
 * Desc:  自定义函数，实现while循环
 */
object Scala11_TestMyWhile {
  def main(args: Array[String]): Unit = {
    /*
    var n = 10
    //while循环
    while (n >= 1){
      println(n)
      n -= 1
    }
    */
    /*
    // 使用柯里化  实现      mywhile (循环条件)(循环体)
    def mywhile(condition: =>Boolean)(op: =>Unit):Unit = {
      if(condition){
        op
        mywhile(condition)(op)
      }
    }
    var n = 10
    mywhile(n>=1){
      println("mywhile-->" + n)
      n -= 1
    }
    */
    /*
    //直接使用普通的函数，实现while循环
    //柯里化好处1：将一个参数列表的多个参数，拆分为多个参数列表，这样参数所表示的含义，清晰、明确
    def mywhile(condition: =>Boolean,op: =>Unit): Unit ={
      if(condition){
        op
        mywhile(condition,op)
      }
    }

    var n = 10
    mywhile(n >=1,{
      println("mywhile-->" + n)
      n -= 1
    })
    */
    //使用闭包的形式，实现mywhile循环
    //外层函数的参数表示循环条件
    //柯里化好处2 ：简化闭包的编写
    def mywhile(condition: =>Boolean): (=>Unit)=>Unit ={
      //内层函数参数表示循环体
      def ff(op: =>Unit): Unit ={
        if(condition){
          op
          mywhile(condition)(op)
        }
      }
      ff _
    }

    var n = 10
    mywhile(n >= 1){
      println("mywhile-->" + n)
      n -= 1
    }

  }
}

