package com.zhangyj.chapter05

object Test {

  def main(args: Array[String]): Unit = {

    // 编写程序实现如下内容：f(10)(20)()输出结果为200
    def f1(x:Int) = {
      def f2(y:Int) = {
        def f3(): Int = {
          return x * y
        }
        f3 _
      }
      f2 _
    }
    println(f1(10)(20)())

    // 编写程序实现如下内容：f(10)(20)(_*_)输出结果为200
    def f4(x:Int) = {
      def f5(y : Int) = {
        def f6(f:(Int,Int) => Int) : Int = {
          f(x,y)
        }
        f6 _
      }
      f5 _
    }
    /*
     * _*_是一个函数，所以f6参数应该定义到函数类型
     * 转换 _*_ 为 (a:Int,b:Int) => (x * y)
     */
    println(f4(10)(20)(_*_))

  }

}
