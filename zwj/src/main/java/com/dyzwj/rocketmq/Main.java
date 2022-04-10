package com.dyzwj.rocketmq;

import com.sun.tools.javac.util.List;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Main {

    private static Main main = new Main();
    static {
        System.out.println("静态代码块");
    }

    public Main(){
        System.out.println("构造函数");
    }

    public static void main(String[] args) {
        DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate start = LocalDate.parse("20200930",yyyyMMdd);
        LocalDate end = LocalDate.parse("20201201",yyyyMMdd);
//        Duration between = Duration.between(start, end);
        Period period = Period.between(start, end);
        System.out.println(period.getDays());

        String a = "abc";
        String b = new String("abc");
        String c = new String("abc").intern();
        System.out.println(a == b);
        System.out.println(b == c);
        System.out.println(a == c);

        test();
    }

    public static void test(){
        List<String> a = List.of("abc","aaa","bca","bo");
        Map<Character, java.util.List<String>> collect = a.stream().collect(Collectors.groupingBy(x -> x.charAt(0)));
        collect.forEach((k,v) -> System.out.println(k + ":" + v.size()));
    }

}
