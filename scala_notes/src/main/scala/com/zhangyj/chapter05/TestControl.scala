package com.zhangyj.chapter05

object TestControl {
  def main(args: Array[String]): Unit = {
    /*
    def f: () => Int = {
      ()=>{
        println("f...")
        10
      }
    }

    def f = {
      ()=>{
        println("f...")
        10
      }
    }

    def f = ()=>{
      println("f...")
      10
    }

    def f = ()=>{
      println("f...")
      10
    }

    foo(f())
    */

    def f = {
      println("f...")
      10
    }
    foo(f)
  }

  //def foo(a: Int):Unit = {
  def foo(a: =>Int):Unit = {//注意这里变量a没有小括号了
    println(a)
    println(a)
  }
}
