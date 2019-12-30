/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.allinone.auto.check.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

    private DateUtil() {
    }

    public static long convertStrToLong4Date(String date) {
        long time = 0l;
        try {
            // 此处HH应该大写，如果小写则输出的是12小时制的时间
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            time = sdf.parse(date).getTime();
        } catch (ParseException e) {
            time = -1l;
            logger.error("[date-convert-error]", e);
        }
        return time;
    }

    public static long convertStrToLong4Date(String date, String pattern) {
        long time = 0l;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            time = sdf.parse(date).getTime();
        } catch (ParseException e) {
            time = -1l;
            logger.error("[date-convert-error]", e);
        }
        return time;
    }

    public static String convertLongToStr4Date(long date) {
        // 此处HH应该大写，如果小写则输出的是12小时制的时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date(date));
    }

    public static boolean isCurrentTimeInInterval(String startTime, String endTime) {
        Date now = new Date();
        // 此处HH应该大写，如果小写则输出的是12小时制的时间
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm");
        String currentTime = dateFormat.format(now);
        return isTimeInInterval(startTime, currentTime, endTime);
    }

    public static boolean isTimeInInterval(String startTime, String middleTime, String endTime) {
        // 需要判断开始时间是否比结束时间大，比如22:00>6:00，次日的可能性
        if (compareTime(startTime, endTime) > 0) {
            return !isTimeInInterval(endTime, middleTime, startTime);
        }

        return compareTime(startTime, middleTime) <= 0 && compareTime(middleTime, endTime) <= 0;
    }

    public static int compareTime(String aTime, String bTime) {
        String[] a = StringUtils.split(aTime, ":");
        String[] b = StringUtils.split(bTime, ":");
        int a_HH = Integer.parseInt(a[0]);
        int a_mm = Integer.parseInt(a[1]);
        int b_HH = Integer.parseInt(b[0]);
        int b_mm = Integer.parseInt(b[1]);
        return (a_HH * 60 + a_mm) - (b_HH * 60 + b_mm);
    }

    public static String diffDate(long diff) {
        long nd = 1000 * 24 * 60 * 60;
        long nh = 1000 * 60 * 60;
        long nm = 1000 * 60;
        long ns = 1000;
        // 计算差多少天
        long day = diff / nd;
        // 计算差多少小时
        long hour = diff % nd / nh;
        // 计算差多少分钟
        long min = diff % nd % nh / nm;
        // 计算差多少秒//输出结果
        long sec = diff % nd % nh % nm / ns;

        StringBuilder ret = new StringBuilder();
        if (day != 0) {
            ret.append(day + "天");
        }
        if (hour != 0) {
            ret.append(hour + "小时");
        }
        if (min != 0) {
            ret.append(min + "分钟");
        }
        if (sec != 0) {
            ret.append(sec + "秒");
        }
        return ret.toString();
    }

    public static String diffDate(long endDate, long startDate) {
        return diffDate(endDate - startDate);
    }

    public static void main(String[] args) {
        long s = DateUtil.convertStrToLong4Date("20180314 14:46:25.231", "yyyyMMdd HH:mm:ss.SSS");
        long e = DateUtil.convertStrToLong4Date("20180310 04:50:25.231", "yyyyMMdd HH:mm:ss.SSS");
        System.out.println(diffDate(s, e));
        System.out.println(diffDate(712400l));
    }
}
