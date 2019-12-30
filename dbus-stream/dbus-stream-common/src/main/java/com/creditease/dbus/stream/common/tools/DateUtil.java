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


package com.creditease.dbus.stream.common.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

    private DateUtil() {
    }

    public static long convertStrToLong4Date(String date, String format) {
        long time = 0l;
        try {
            // 此处HH应该大写，如果小写则输出的是12小时制的时间
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            time = sdf.parse(date.trim()).getTime();
        } catch (ParseException e) {
            logger.info("convertStrToLong4Date failed: {}", e);
            time = -1l;
        }
        return time;
    }

    public static long addDay(String data, String format, int hours) {
        long time = 0L;
        try {
            // 此处HH应该大写，如果小写则输出的是12小时制的时间
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Date date = sdf.parse(data.trim());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.HOUR, hours);
            time = calendar.getTimeInMillis();
        } catch (ParseException e) {
            logger.info("convertStrToLong4Date failed: {}", e);
            time = -1l;
        }
        return time;
    }

    public static String convertLongToStr4Date(long date) {
        // 此处HH应该大写，如果小写则输出的是12小时制的时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date(date));
    }

    public static long getTimeMills(String timeStr) throws ParseException {
        String ptn = "yyyy-MM-dd HH:mm:ss.SSS";
        timeStr = timeStr.substring(0, 23);
        if (timeStr.length() == 19) {
            ptn = "yyyy-MM-dd HH:mm:ss";
        }

        DateFormat df = new SimpleDateFormat(ptn);
        return df.parse(timeStr).getTime();
    }

    public static void main(String[] args) {
        String value = "2018-06-29-15:59:03.971000";
        String format = "yyyy-MM-dd-HH:mm:ss.SSSSSS";
        Long time = convertStrToLong4Date(value, format);
        String date = convertLongToStr4Date(time);

        System.out.println("date[Long] is " + time);
        System.out.println("date[string:yyyy-MM-dd HH:mm:ss.SSS] is " + date);
    }


}
