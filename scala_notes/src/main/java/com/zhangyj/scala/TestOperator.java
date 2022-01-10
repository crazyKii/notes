package com.zhangyj.scala;

/**
 * Author: Felix
 * Date: 2020/4/25
 * Desc:
 */
public class TestOperator {
    public static void main(String[] args) {
        /*String s1 = new String("abc");
        String s2 = "abc";
        System.out.println(s1 == s2);//false
        System.out.println(s1.equals(s2));//true*/

//        System.out.println(isNotEmpty(null));
//        System.out.println(jc(5));

//        byte b = 10;
////        b = b + 1;
//        b += 1 ;
//        System.out.println(b);


        // ++运算符
        // ++在前  ，先运算再赋值
//        int x = 10;
//        int y = ++ x;

//        System.out.println("x=" + x + ",y=" + y);
        // ++在后  ，先赋值再运算
//        int x = 10;
//        int y = x ++;
//        System.out.println("x=" + x + ",y=" + y);

        /*
        *   1.  定义一个临时变量，接收等号右边的值  temp = x ++  ,temp = 10
        *   2.  x自增操作， x ++ = 11
        *   3.  将等号右边的值，赋值给等号的左边，x = temp = 10
        * */
        int x = 10;
        x = x ++;
        System.out.println(x);  //10
    }
   /*
   public static boolean isNotEmpty(String s){
        return s!=null && !"".equals(s.trim());
   }
    */
  /*  public static int jc(int n){
        if(n==1){
            return 1;
        }
        return n * jc(n--);
    }*/
}
