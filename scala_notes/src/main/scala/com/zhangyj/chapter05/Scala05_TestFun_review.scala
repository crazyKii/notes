package com.zhangyj.chapter05

/**
 * Date: 2020/4/28
 * Desc: 函数至简原则以及匿名函数回顾
 */
object Scala05_TestFun_review {
  def main(args: Array[String]): Unit = {
    /*
    //正常定义函数
    def f0(name:String): String ={
      return name
    }
    println(f0("atguigu"))
    //（1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    def f1(name:String): String ={
      name
    }
    println(f1("atguigu"))

    //（2）如果函数体只有一行代码，可以省略花括号
    def f2(name:String): String = name
    println(f2("atguigu"))

    //（3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def f3(name:String)= name
    println(f3("atguigu"))

    //（4）如果有return，则不能省略返回值类型，必须指定
    def f4(name:String) ={
      return name
    }

    //（5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    def f5(name:String): Unit ={
      return name
    }
    println(f5("atguigu"))

    //（6）Scala如果期望是无返回值类型，可以省略等号   这种形式称为过程
    def f6(name:String){
      println(name)
    }
    f6("banzhang")

    //（7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def f7(): Unit ={
      println("jingjing")
    }
    f7
    f7()

    //（8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def f8: Unit ={
      println("jingjing")
    }
    //f8()
    f8
    */

    //（9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    // 1.扩展f9的功能   2.具体扩展的功能是通过fun函数传递，很灵活
    def f9(fun:(String)=>Unit): Unit ={
      fun("jingjing")
    }

    //def f10(s:String): Unit ={
    //  println(s)
    //}
    //f9(f10)

    //匿名函数 ：没有名字的函数，通过lambda表达式实现     (参数)=>{函数体}
    //f9((s:String)=>{println(s)})

    //（1）参数的类型可以省略，会根据形参进行自动的推导
    //f9((s)=>{println(s)})

    //（2）类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过1的永远不能省略圆括号。
    //f9(s =>{println(s)})

    //（3）匿名函数如果只有一行，则大括号也可以省略
    //f9(s =>println(s))

    //（4）如果参数只出现一次，则参数省略且后面参数可以用_代替
    //f9(println(_))

    //  （5） 如果可以推断出，println是一个函数体，而不是调用语句，那么(_)可以省
    //  f9(println)

    //反推导     f9((s:String)=>println(s))

    //加深大家理解
    //定义一个函数，接受一个函数类型的参数，该函数类型有两个参数
    def f11(fun:(Int,Int)=>Int):Int={
      fun(1,2)
    }

    //println(f11((x:Int,y:Int)=>{x + y}))
    //println(f11((x,y)=>x + y))
    println(f11(_ + _))

  }
}
