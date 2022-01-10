package com.zhangyj.chapter02

/**
  * Desc: 类型转换
  *   -自动类型转换
  *   -强转
  */
object Scala07_DataType {
  def main(args: Array[String]): Unit = {

    //（1）自动提升原则：有多种类型的数据混合运算时，系统首先自动将所有数据转换成精度大的那种数据类型，然后再进行计算。
    //var b1:Byte = 10
    //var n:Long = 20L
    //var res:Long = n + b1

    //（2）把精度大的数值类型赋值给精度小的数值类型时，就会报错，反之就会进行自动类型转换。
    //var b:Byte = 10
    //var n:Int = b   //将精度小的数据赋值给精度大的类型，会自定进行转换（隐式转换）
    //var b1:Byte = n //把精度大的数值类型赋值给精度小的数值类型时，就会报错

    //（3）（byte，short）和char之间不会相互自动转换。
    //var b:Byte = 10
    ////var c:Char = b
    //var c:Char = 96
    //var n:Int = c

    //（4）byte，short，char他们三者可以计算，在计算时首先转换为int类型。
    //var b:Byte = 10
    //var s:Short = 20
    //var c:Char = 30
    //val res: Int = b + s + c


    //强制类型转换
    //（1）将数据由高精度转换为低精度，就需要使用到强制转换
    //var n:Int = 2.5.toInt
    //println(n)

    //（2）强转符号只针对于最近的操作数有效，往往会使用小括号提升优先级
    //var n:Int = 2.5.toInt + 3.6.toInt
    //var n:Int = (2.5 + 3.6).toInt

    //数值类型和String类型间转换
    //数值 -->字符串
    //var n = 10 + ""

    //字符串-->数值
    //var res = "12".toInt
    var res = "12.3".toDouble
    println(res)

  }
}
