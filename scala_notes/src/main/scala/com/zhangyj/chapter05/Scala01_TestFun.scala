package com.zhangyj.chapter05

/**
 * Date: 2020/4/27
 * Desc: 函数的基本用法
 *   -函数和方法
 *     *函数
 *         完成某个特殊功能的代码块
 *     *函数和方法定义的位置不一样
 *       函数:定义在方法或者函数内部
 *       方法:定义在类下的函数，叫方法
 *
 *     *方法支持重写和重载，但是函数不允许
 *         重载对调用者可以屏蔽掉因为方法参数不同而造成差异
 */
object Scala01_TestFun {

  // 定义一个方法
  def sayHi(name:String):String = {
    return "hello-->" + name
  }

  // 重写方法
  def sayHi(age:Int):String = {
    return "hello-->" + age
  }

  def main(args: Array[String]): Unit = {

    //定义一个函数
    def sayHi(name:String):String = {
      return "hello-->" + name
    }

    //调用函数
    println(sayHi("jingjing"))
  }
}
