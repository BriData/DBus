/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.heartbeat.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.creditease.dbus.heartbeat.log.LoggerFactory;
import org.apache.commons.lang.StringUtils;

public class DateUtil {

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
            LoggerFactory.getLogger().error("[date-convert-error]", e);
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
}
