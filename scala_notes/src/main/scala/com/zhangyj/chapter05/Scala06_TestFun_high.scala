package com.zhangyj.chapter05

/**
 * Date: 2020/4/28
 * Desc: 高阶函数
 */
object Scala06_TestFun_high {
  def main(args: Array[String]): Unit = {
    /*
    //函数正常的声明与调用
    def foo():Int={
      println("foo...")
      10
    }
    //将foo函数的执行结果赋值给res变量
    //val res: Int = foo()
    //println(res)

    //函数作为值进行传递  语法：在函数名称的后面  +  空格  加下划线
    //注意：将foo函数作为一个整体，赋值给f变量，  f是函数类型 ()=>Int
    //val f : () => Int = foo _
    def f : () => Int = foo _


    //这个时候，f就是一个函数，如果要想运行f函数的话，必须得加()
    //println(f)   //<function0>
    //println("-----------------")
    //println(f())

    //var ff = foo   //将函数执行结果赋值给ff
    //var ff = foo _   //将函数本身作为值赋给ff

    //将函数本身作为值赋给ff    如果明确了变量的类型，那么空格和下划线可以省略
    //var ff:()=>Unit = foo
    //println(ff)

    */
    /*
    //函数可以作为参数，进行传递（大多数情况都是通过匿名函数的形式）
    //定义一个函数calculator，该函数接收一个函数类型的参数op，op定义了对两个整数要进行什么样的操作
    def calculator(a:Int,b:Int,op:(Int,Int)=>Int): Int ={
      op(a,b)
    }

    //定义一个函数f1,完成两个整数的加法运算
    def op(a:Int,b:Int):Int={
      a + b
    }
    //println(calculator(10,20,op))
    println(calculator(50, 20, (a: Int, b: Int) => {
      a - b
    }))

    println(calculator(50, 20, _ - _))
    println(calculator(50, 20, _ + _))
    */

    //函数可以作为返回值进行返回----函数的嵌套
    def f1():()=>Unit ={
      def f2(): Unit ={
        println("f2函数被调用了")
      }
      //将f2作为返回值返回给f1
      f2 _
    }

    //ff就是f2
    //var ff = f1()
    //ff()

    //f1()()

    //定义一个变量，变量名称为fun；
    //变量的类型是函数，函数有三个参数分别为Int，String，Char，返回值为Boolean

  }
}
