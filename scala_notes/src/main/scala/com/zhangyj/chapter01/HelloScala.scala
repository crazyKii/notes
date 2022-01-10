package com.zhangyj.chapter01

/**
 * - object
 *   从语法的角度讲，上面声明了一个伴生对象
 * - 伴生对象
 *   * 伴生类产生的一个对象
 *   * 当我们对源文件进行编译之后，默认会生成2个字节码，一个是伴生对象，一个是伴生对象所属类
 *
 *   ---------------------对第一个Scala案例进行说明----------------------
 *    >object
 *      关键字，表示声明一个伴生对象
 *    >Scala01_HelloWorld
 *      伴生对象的名字，取名的时候需要符合标识符命名规则
 *    >def
 *      关键字  标识声明一个方法
 *    >main
 *      方法的名称
 *    >(args: Array[String])
 *      args 参数名称
 *      Array[String]参数类型,在Scala语言中，[]表示泛型
 *      声明参数的时候，名称在前，类型在后，名称和类型之间用冒号分隔
 *    >Unit
 *      返回值类型为空，相当于java语言中的void关键字
 *      Unit是一个类型，当前类型只有一个实例()
 *      参数列表和返回值类型之间，用冒号进行分隔
 *      返回值类型和函数体之间用等号进行连接
 *    > println("HelloScala")
 *      向控制台打印输出内容
 *      在Scala语言中，语句结束不需要加分号
 */
object HelloScala {
  def main(args: Array[String]): Unit = {
  println("hello,Scala")
  }


  /*
  完全面向对象：scala完全面向对象，故scala去掉了java中非面向对象的元素，如static关键字，void类型
  1.static
  scala无static关键字，由object实现类似静态方法的功能（类名.方法名），object关键字和class的关键字定义方式相同，但作用不同。class关键字和java中的class关键字作用相同，用来定义一个类；object的作用是声明一个单例对象，object后的“类名”可以理解为该单例对象的变量名。
  2.void
  对于无返回值的函数，scala定义其返回值类型为Unit类
  */
}
