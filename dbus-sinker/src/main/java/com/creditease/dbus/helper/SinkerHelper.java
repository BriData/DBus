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


package com.creditease.dbus.helper;

import com.creditease.dbus.bean.HdfsOutputStreamInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class SinkerHelper {

    private static Logger logger = LoggerFactory.getLogger(SinkerHelper.class);
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    /**
     * 获取hdfs的下一个文件名
     */
    public static String getHdfsFileName(long opts) {
        Random r = new Random();
        int number = r.nextInt(9000) + 1000;
        return sdf.format(opts) + number;
    }

    public static String getHdfsFilePath(String[] dataKeys, String hdfsRootPath, String fileName) {
        return String.format("%s/%s.%s.%s/%s/%s/0/0/data_increment_data/right/%s", hdfsRootPath, dataKeys[1], dataKeys[2],
                dataKeys[3], dataKeys[4], dataKeys[5], fileName).toLowerCase();
    }


    public static boolean needCreateNewFile(Long opts, String version, HdfsOutputStreamInfo hdfsOutputStreamInfo) {
        String v = hdfsOutputStreamInfo.getVersion();
        //跨版本需要切换文件
        if (!StringUtils.equals(version, v)) {
            logger.info("版本变更,需要切换hdfs文件,上一个版本:{},当前版本:{}", v, version);
            return true;
        }
        //跨天需要切换文件
        String fileName = hdfsOutputStreamInfo.getFileName();
        String day = StringUtils.substring(fileName, 0, 8);
        String curDay = StringUtils.substring(SinkerHelper.getHdfsFileName(opts), 0, 8);
        if (!StringUtils.equals(day, curDay)) {
            logger.info("日期变更,需要切换hdfs文件,上一个日期:{},当前日期:{}", day, curDay);
            return true;
        }
        return false;
    }

    /**
     * 获取当天的零时零分零秒
     */
    public static String getStartTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 000);
        return sdf.format(calendar.getTime());
    }
}
