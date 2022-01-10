package com.zhangyj.scala;

/**
 * Author: Felix
 * Date: 2020/4/27
 * Desc:
 */
//静态导入
import static java.lang.Math.*;
public class TestBreak {
    public static void main(String[] args) {


        System.out.println(10);
        System.out.println("abc");
        System.out.println(PI);
        System.out.println(abs(-5));
       /* try{
            //模拟break关键字，终止整个循环
            for (int i = 1; i <=5; i++) {
                if(i == 3){
                    //break;
                    //i = 6;
                    throw new RuntimeException();
                }
                System.out.println(i);
            }
        }catch(Exception e){

        }


        System.out.println("这是循环外的代码");*/

    }
}
