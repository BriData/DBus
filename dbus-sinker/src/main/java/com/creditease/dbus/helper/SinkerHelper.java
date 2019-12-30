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
    public static String getHdfsFileName() {
        Random r = new Random();
        int number = r.nextInt(9000) + 1000;
        return sdf.format(new Date()) + number;
    }

    public static String getHdfsFilePath(String[] dataKeys, String hdfsRootPath, String fileName) {
        return String.format("%s/%s.%s.%s/%s/%s/0/0/data_increment_data/right/%s", hdfsRootPath, dataKeys[1], dataKeys[2],
                dataKeys[3], dataKeys[4], dataKeys[5], fileName).toLowerCase();
    }


    public static boolean needCreateNewFile(String version, String path) {
        String[] split = StringUtils.split(path, "/");
        if (StringUtils.equals(version, split[split.length - 6])) {
            String fileName = split[split.length - 1];
            //跨天需要切换文件
            if (fileName.compareTo(getStartTime() + "0000") <= 0) {
                return true;
            } else {
                return false;
            }
        } else {
            //版本变更需要切换文件
            return true;
        }
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
