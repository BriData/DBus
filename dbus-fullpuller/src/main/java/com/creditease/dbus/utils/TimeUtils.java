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


package com.creditease.dbus.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/30
 */
public class TimeUtils {
    private static Logger logger = LoggerFactory.getLogger(TimeUtils.class);

    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static final int nd = 1000 * 24 * 60 * 60;
    public static final int nh = 1000 * 60 * 60;
    public static final int nm = 1000 * 60;
    public static final int ns = 1000;

    public static String getCurrentTimeStampString() {
        String timeStampString = formatter.format(new Date());
        return timeStampString;
    }

    public static Long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    public static void sleep(long time) {
        try {
            TimeUnit.MILLISECONDS.sleep(time);
        } catch (InterruptedException e) {
            logger.warn("sleep exception {}", e.getMessage());
        }
    }

    public static String formatTime(long time) {

        // 计算差多少天
        int day = (int) time / nd;
        // 计算差多少小时
        int hour = (int) time % nd / nh;
        // 计算差多少分钟
        int min = (int) time % nd % nh / nm;
        // 计算差多少秒//输出结果
        int sec = (int) time % nd % nh % nm / ns;
        // 计算差多少毫秒//输出结果
        int ms = (int) time % ns;

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
        if (sec != 0) {
            ret.append(ms + "毫秒");
        }
        return ret.length() == 0 ? "0毫秒" : ret.toString();
    }

}
