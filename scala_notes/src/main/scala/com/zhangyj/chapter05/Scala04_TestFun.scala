package com.zhangyj.chapter05


/**
 * Date: 2020/4/27
 * Desc: 函数至简原则（重点）
 *      -能省则省
 */
object Scala04_TestFun {
  def main(args: Array[String]): Unit = {
    /*
    //函数正常定义与调用
    def sayHi(name:String): String ={
      return name
    }
    println(sayHi("atguigu"))

    //（1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    def f1(name:String): String ={
      name
    }
    println(f1("atguigu"))

    //（2）如果函数体只有一行代码，可以省略花括号
    def f2(name:String): String = name
    println(f2("atguigu"))

    //（3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def f3(name:String) = name
    println(f3("atguigu"))

    //（4）如果有return，则不能省略返回值类型，必须指定
    def f4(name:String) = {
      return name
    }
    println(f4("atguigu"))

    //（5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    def f5(name:String): Unit ={
      return name
    }
    println(f5("atguigu"))

    //（6）Scala如果期望是无返回值类型，可以省略等号   这种写法称为过程
    def f6(name:String){
      name
    }
    println(f6("atguigu"))

    //（7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def f7(): Unit ={
      println("hello")
    }
    f7
    f7()

    //（8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def f8 : Unit ={
      println("hello")
    }
    f8
    f8()
    */

    //（9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    //函数类型   (参数列表声明)=>返回值
    def f10(s:String): Unit ={
      println(s)
    }

    def f9(f: (String)=>Unit): Unit ={
      f("atguigu")
    }
    f9(f10)
    f9(f10 _)

    // 匿名函数   通过Lambda表达式实现    (参数列表)->{函数体}
    // f9((s:String)=>{println(s)})
    //（1）参数的类型可以省略，会根据形参进行自动的推导
    // f9((s)=>{println(s)})
    //（2）类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过1的永远不能省略圆括号。
    // f9(s=>{println(s)})
    //（3）匿名函数如果只有一行，则大括号也可以省略
    // f9(s=>println(s))
    //（4）如果参数只出现一次，则参数省略且后面参数可以用_代替
    // f9(println(_))

    // 如果在进行println输出的时候，可以推断出是一个函数,那么下划线可以省略
    // f9(println)


  }
}

