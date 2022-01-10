package com.zhangyj.chapter05

/**
 * Date: 2020/4/28
 * Desc: 控制抽象
 *   -值调用
 *     将函数计算后的值进行传递
 *   -名调用
 *     传递的是代码块
 */
object Scala10_TestControlAbstract {
  def main(args: Array[String]): Unit = {
    /*
    //值调用  将函数计算后的值进行传递
    def foo(a:Int):Unit={
      println(a)
      println(a)
    }

    def f(): Int = {
      println("f...")
      10
    }

    //f...
    //10
    //10
    foo(f())

    //名调用 传递的是代码块
    // 数据类型    Int，String，函数 参数类型=>返回值，代码块   =>返回值类型

    def foo(a : =>Int):Unit={
      println(a)
      println(a)
    }

    def f(): Int = {
      println("f...")
      10
    }

    //f...
    //10
    //f...
    //10
    foo(f)
    */

    def f1(ss: =>String): Unit ={
      println(ss)
    }

    f1(
      {
        println("atguigu")
        "jingjing"
      }
    )

  }
}
