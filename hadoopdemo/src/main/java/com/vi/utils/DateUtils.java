package com.vi.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtils {

    public final static String pattern1 = "yyyy-MM-dd HH:mm:ss";

    public static String dateToString(Date date, String pattern1) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern1);
        return sdf.format(date);
    }

    public static Date datePlus(Date date, int num) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(calendar.DATE, num); //把日期往后增加一天,整数  往后推,负数往前移动
        date = calendar.getTime(); //这个时间就是日期往后推一天的结果
        return date;
    }

    public static void main(String[] args) {
        System.out.println(dateToString(datePlus(new Date(), 1), DateUtils.pattern1));
    }
}
