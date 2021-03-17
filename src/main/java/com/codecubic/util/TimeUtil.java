package com.codecubic.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

@Slf4j
public class TimeUtil {

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYYMMDD = "yyyyMMdd";

    public static Date str2Date(String dateStr, String format) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        LocalDateTime parse = LocalDateTime.parse(dateStr, dtf);
        return Date.from(parse.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Date localtime2Date(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDateTime date2LocalTime(Date date) {
        LocalDateTime ldt = date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return ldt;
    }

    public static String date2Str(Date date, String format) {
        return dateTime2Str(date2LocalTime(date), format);
    }

    public static String dateTime2Str(LocalDateTime localDateTime, String format) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
        return df.format(localDateTime);
    }

    public static Date currentTimeToDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format1 = df.format(new Date());
        return str2Date(format1, YYYY_MM_DD_HH_MM_SS);
    }

    public static boolean after(Date date1, Date date2) {
        log.error("date1:{},date2:{}", date1, date2);
        if (date1 == null || date2 == null) {
            return false;
        }
        return date1.after(date2);
    }

    public static void sleepSec(Integer sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void sleepMill(Integer mill) {
        try {
            Thread.sleep(mill);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断对象是否为null，是null则睡眠sec秒，最多睡眠num次
     */
    public static void nullSleepSec(Field field, Object obj, int num, int sec) throws IllegalAccessException {
        while (field.get(obj) == null && num > 0) {
            sleepSec(sec);
            num--;
        }
    }
}
