package com.zhangyj.chapter02

/**
 * Java
 *  变量类型 变量名称 = 初始值，int a = 10
 *  final常量类型 常量名称 = 初始值，final int b = 20
 *
 * Scala
 *  var 变量名 [: 变量类型] = 初始值，var i:Int = 10
 *  val 常量名 [: 常量类型] = 初始值，val j:Int = 20
 */
object Scala02_Var {

  def main(args: Array[String]): Unit = {

    //（1）声明变量时，类型可以省略，编译器自动推导，即类型推导
    var age = 18
    age = 30

    //（2）类型确定后，就不能修改，说明Scala是强数据类型语言。
    // age = "tom" // 错误

    //（3）变量声明时，必须要有初始值
    // var name //错误

    //（4）在声明/定义一个变量时，可以使用var或者val来修饰，var修饰的变量可改变，val修饰的变量不可改。
    var num1 = 10   // 可变
    val num2 = 20   // 不可变

    num1 = 30  // 正确
    //num2 = 100  //错误，因为num2是val修饰的


    //（5）var修饰的对象引用可以改变，val修饰的对象则不可改变，但对象的状态（值）却是可以改变的。（比如：自定义对象、数组、集合等等）
    // p1是var修饰的，p1的属性可以变，而且p1本身也可以变
    var p1 = new Person()
    p1.name = "dalang"
    p1 = null

    // p2是val修饰的，那么p2本身就不可变（即p2的内存地址不能变），但是，p2的属性是可以变，因为属性并没有用val修饰。
    val p2 = new Person()
    p2.name="jinlian"
    // p2 = null // 错误的，因为p2是val修饰的
  }
}

class Person{
  var name : String = "jinlian"
}
