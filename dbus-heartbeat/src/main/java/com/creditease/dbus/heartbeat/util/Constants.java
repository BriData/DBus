/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

public class Constants {

    private Constants() {
    }

    public static final String SYS_PROPS_LOG_TYPE = "log.type";

    public static final String SYS_PROPS_LOG4J_CONFIG = "log4j.configuration";

    public static final String SYS_PROPS_LOG_BASE_PATH = "logs.base.path";

    public static final String SYS_PROPS_LOG_HOME = "logs.home";

    public static final String CONFIG_DB_KEY = "dbus.conf";

    public static final String CONFIG_DB_TYPE_ORA = "oracle";

    public static final String CONFIG_DB_TYPE_MYSQL = "mysql";


    public static final String CONFIG_KAFKA_CONTROL_PAYLOAD_SCHEMA_KEY = "schema";

    public static final String CONFIG_KAFKA_CONTROL_PAYLOAD_DB_KEY = "DB";

    public static final String GLOBAL_CTRL_TOPIC = "global_ctrl_topic";


    /** 心跳邮件报警内容 */
    public static final String MAIL_HEART_BEAT = "尊敬的先生/女士您好，Dbus心跳监控发现数据线路:{0}发生异常，报警次数:{1},超时次数:{2},请及时处理.";
    /** 全量邮件报警内容 */
    public static final String MAIL_FULL_PULLER = "尊敬的先生/女士您好，Dbus全量拉取监控发现拉取表:{0}数据时发生异常，报警次数:{1},超时次数:{2},请及时处理.";
    public static final String MAIL_FULL_PULLER_NEW = "您好:</br>" +
                                                      "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                      "&nbsp;&nbsp;数据源:{1}</br>" +
                                                      "&nbsp;&nbsp;数据库:{2}</br>" +
                                                      "&nbsp;&nbsp;表:{3}</br>" +
                                                      "&nbsp;&nbsp;版本:{4}</br>" +
                                                      "&nbsp;&nbsp;报警时间:{5}</br>" +
                                                      "&nbsp;&nbsp;报警环境:{6}</br>" +
                                                      ",请及时处理.</br></br>" +
                                                      "详细信息:</br>{7}";

    public static final String MAIL_FULL_PULLER_NEW_PROJECT = "您好:</br>" +
                                                              "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                              "&nbsp;&nbsp;项目名:{1}</br>" +
                                                              "&nbsp;&nbsp;数据源:{2}</br>" +
                                                              "&nbsp;&nbsp;数据库:{3}</br>" +
                                                              "&nbsp;&nbsp;表:{4}</br>" +
                                                              "&nbsp;&nbsp;版本:{5}</br>" +
                                                              "&nbsp;&nbsp;报警时间:{6}</br>" +
                                                              "&nbsp;&nbsp;报警环境:{7}</br>" +
                                                              ",请及时处理.</br></br>" +
                                                              "详细信息:</br>{8}";

    /** 新版心跳邮件报警内容 */
    // public static final String MAIL_HEART_BEAT_NEW = "尊敬的先生/女士您好:</br>&nbsp;&nbsp;Dbus心跳监控发现数据源:{0}</br>&nbsp;&nbsp;schema:{1}</br>以下表格中table发生超时,请及时处理.</br></br>{2}";
    public static final String MAIL_HEART_BEAT_NEW = "您好:</br>" +
                                                     "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                     "&nbsp;&nbsp;数据源:{1}</br>" +
                                                     "&nbsp;&nbsp;数据库:{2}</br>" +
                                                     "&nbsp;&nbsp;报警时间:{3}</br>" +
                                                     "&nbsp;&nbsp;报警环境:{4}</br>" +
                                                     "以下表格中table发生报警,请及时处理.</br></br>{5}";
    /** 数据库主备延时报警邮件内容 */
    // public static final String MAIL_MASTER_SLAVE_DELAY = "尊敬的先生/女士您好:</br>&nbsp;&nbsp;Dbus心跳监控发现数据源:{0}</br>&nbsp;&nbsp;schema:{1}</br>所在主备数据库延时过长发生超时,请及时处理.";
    public static final String MAIL_MASTER_SLAVE_DELAY = "您好:</br>" +
                                                         "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                         "&nbsp;&nbsp;数据源:{1}</br>" +
                                                         "&nbsp;&nbsp;数据库:{2}</br>" +
                                                         "&nbsp;&nbsp;主备延时:{3}</br>" +
                                                         "&nbsp;&nbsp;主库时间:{4}</br>" +
                                                         "&nbsp;&nbsp;备库时间:{5}</br>" +
                                                         "&nbsp;&nbsp;报警环境:{6}</br>" +
                                                         "&nbsp;&nbsp;报警时间:{7}</br>" +
                                                         "请及时处理.";

    public static final String MAIL_SCHEMA_CHANGE = "您好:</br>" +
                                                    "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                    "&nbsp;&nbsp;数据源:{1}</br>" +
                                                    "&nbsp;&nbsp;数据库:{2}</br>" +
                                                    "&nbsp;&nbsp;表名:{3}</br>" +
                                                    "&nbsp;&nbsp;报警时间:{4}</br>" +
                                                    "&nbsp;&nbsp;报警环境:{5}</br>" +
                                                    "请及时处理.</br></br>{6}";
    /** 新版心跳邮件报警内容 */
    // public static final String MAIL_HEART_BEAT_NEW = "尊敬的先生/女士您好:</br>&nbsp;&nbsp;Dbus心跳监控发现数据源:{0}</br>&nbsp;&nbsp;schema:{1}</br>以下表格中table发生超时,请及时处理.</br></br>{2}";
    public static final String MAIL_HEART_BEAT_NEW_PROJECT = "您好:</br>" +
                                                            "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                            "&nbsp;&nbsp;项目:{1}</br>" +
                                                            "&nbsp;&nbsp;拓扑:{2}</br>" +
                                                            "&nbsp;&nbsp;数据源:{3}</br>" +
                                                            "&nbsp;&nbsp;数据库:{4}</br>" +
                                                            "&nbsp;&nbsp;报警时间:{5}</br>" +
                                                            "&nbsp;&nbsp;报警环境:{6}</br>" +
                                                            "以下表格中table发生报警,请及时处理.</br></br>{7}";

    public static final String PROJECT_EXPIRE_TIME = "您好:</br>" +
                                                            "&nbsp;&nbsp;报警类型:{0}</br>" +
                                                            "&nbsp;&nbsp;项目名称:{1}</br>" +
                                                            "&nbsp;&nbsp;有效期至:{2}</br>" +
                                                            "&nbsp;&nbsp;剩余:{3}天</br>" +
                                                            "&nbsp;&nbsp;报警环境:{4}</br>" +
                                                            "{5}. 请及时处理.</br>";


}
