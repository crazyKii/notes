package com.zhangyj.chapter02

/**
  * Desc:  字符串输出
  */
object Scala04_TestString {
  def main(args: Array[String]): Unit = {
    //（1）字符串，通过+号连接
    var name:String = "jingjing"
    var age:Int = 18
    //println(age + "岁的"+ name +"在200105班级学习")
    println(s"${age}岁的${name}在200105班级学习")

    //（2）printf用法：字符串，通过%传值。
    //printf("%d岁的%s在200105班级学习",age,name)

    //（3）字符串模板（插值字符串）：通过$获取变量值
    // 保持字符串原格式显示与输出
    // *多行字符串，在Scala中，利用三个双引号包围多行字符串就可以实现。
    // *输入的内容，带有空格、\t之类，导致每一行的开始位置不能整洁对齐。
    // *应用scala的stripMargin方法，在scala中stripMargin默认是“|”作为连接符，在多行换行的行头前面加一个“|”符号即可。
    // *如果需要对变量进行运算，那么可以加${}
    var sql:String =
      s"""
        |select
        |	*
        |from
        |  student
        |where
        |	name = ${name}
        |and
        |	age = ${age + 2}
      """.stripMargin
    println(sql)

    val hei = 2.345
    println(f"The height is ${hei%2.2f}")
  }
}
