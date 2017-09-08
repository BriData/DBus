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

    public static final String DB_DRIVER_CLASS_ORA = "oracle.jdbc.driver.OracleDriver";

    public static final String DB_DRIVER_CLASS_MYSQL = "com.mysql.jdbc.Driver";

    public static final String GLOBAL_CTRL_TOPIC = "global_ctrl_topic";

    /** 心跳邮件报警内容 */
    public static final String MAIL_HEART_BEAT = "尊敬的先生/女士您好，Dbus心跳监控发现数据线路:{0}发生异常，报警次数:{1},超时次数:{2},请及时处理.";
    /** 全量邮件报警内容 */
    public static final String MAIL_FULL_PULLER = "尊敬的先生/女士您好，Dbus全量拉取监控发现拉取表:{0}数据时发生异常，报警次数:{1},超时次数:{2},请及时处理.";

}
