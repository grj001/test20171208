package com.zhiyou.test;

import java.util.Random;

/**
 * @author ${user.name}
 */
public class AppTest {


    public static void main(String[] args) {

        //选择排序

        //4行, 4列
        //4, 3, 2, 1
        //  5 个元素

        //  01  12  23  34
        //  01  12  23
        //  01  12
        //  01
        Random random = new Random();
        int[] arr = {
                random.nextInt(100)
                , random.nextInt(100)
                , random.nextInt(100)
                , random.nextInt(100)
                , random.nextInt(100)
        };

        for(int a : arr){
            System.out.println(a);
        }

        for(int i=0; i<arr.length-1; i++) {
            for(int j=0; j<arr.length-1-i; j++){
                if(arr[j]>arr[j+1]){
                    arr[j] = arr[j]+arr[j+1]-(arr[j+1]=arr[j]);
                }

            }
        }

        System.out.println("------------");
        for(int a : arr){
            System.out.println(a);
        }
    }

}
