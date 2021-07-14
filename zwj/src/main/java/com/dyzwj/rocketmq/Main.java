package com.dyzwj.rocketmq;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class Main {

    public static void main(String[] args) {
        DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate start = LocalDate.parse("20200930",yyyyMMdd);
        LocalDate end = LocalDate.parse("20201201",yyyyMMdd);
//        Duration between = Duration.between(start, end);
        Period period = Period.between(start, end);
        System.out.println(period.getDays());

    }
}
