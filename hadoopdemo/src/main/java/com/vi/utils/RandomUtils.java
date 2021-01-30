package com.vi.utils;

import java.util.Random;

public class RandomUtils {

    public static int getRandomNum(int limit) {
        Random r = new Random();
        return r.nextInt(limit);
    }

    public static void main(String[] args) {
        System.out.println(getRandomNum(10));
    }

}
