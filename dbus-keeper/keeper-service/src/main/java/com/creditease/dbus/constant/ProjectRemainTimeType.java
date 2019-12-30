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


package com.creditease.dbus.constant;

/**
 * User: 王少楠
 * Date: 2018-07-20
 * Desc: 用来标识项目剩余时间
 */
public enum ProjectRemainTimeType {
    NORMAL("normal"), MONTH("month"), BEFORE("before"), EXPIRE("expire"), UNKNOWN("unknown");

    /**
     * 一天的毫秒数
     */
    public static final long ONE_DAY = 1000 * 60 * 60 * 24;
    /**
     * 每个月的天数（月通知的时间,可以自定义,也就是第一次通知是提前 MONTH_NUM 天的时候）
     */
    public static final long MONTH_NUM = 30;
    /**
     * 根据第一次通知天数,计算出时间
     */
    public static final long MONTH_DAY = ONE_DAY * MONTH_NUM;
    /**
     * 5天的天数（可以自定义,也就是邻到期BEFORE_NUM 天的时候通知）
     */
    public static final long BEFORE_NUM = 5;
    /**
     * 根据最后一次通知计算时间
     */
    public static final long BEFORE_DAY = ONE_DAY * BEFORE_NUM;


    /**
     * 将数据的字段,转化成类型
     *
     * @param time
     * @return
     */
    public static ProjectRemainTimeType parse(String time) {
        switch (time) {
            case "normal":
                return NORMAL;
            case "month":
                return MONTH;
            case "before":
                return BEFORE;
            case "expire":
                return EXPIRE;
            default:
                return UNKNOWN;
        }
    }


    String time;

    private ProjectRemainTimeType(String time) {
        this.time = time;
    }

    public String getTime() {
        return time;
    }
}
