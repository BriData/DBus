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
 * Date: 2018-06-15
 * Time: 下午4:43
 */
public enum TopologyType {
    //0.2.x appender  dispatcher  puller  splitter  splitter_puller
    //0.3.x appender  dispatcher_appender  mysql_extractor  splitter dispatcher  log_processor  puller  splitter_puller
    //0.4.x appender  dispatcher  dispatcher_appender  log_processor  mysql_extractor  splitter_puller
    APPENDER("appender"),
    DISPATCHER("dispatcher"),
    DISPATCHER_APPENDER("dispatcher_appender"),

    SPLITTER_PULLER("splitter_puller"),
    PULLER("puller"),
    SPLITTER("splitter"),

    LOG_PROCESSOR("log_processor"),
    MYSQL_EXTRACTOR("mysql_extractor"),

    ROUTER("router"),

    SINKER("sinker");

    private String value;

    TopologyType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
