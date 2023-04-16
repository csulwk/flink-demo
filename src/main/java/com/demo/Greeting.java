package com.demo;

/**
 * @author kai
 * @date 2023-04-15 17:05
 */
public class Greeting {

    public String sayGreet(String name) {
        return String.join(":", "111", name);
    }

    public static void sayHello(String name) throws Exception {
        Thread.sleep(2000);
        System.out.println("111! name = " + name);
    }
}
