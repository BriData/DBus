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


package com.creditease.dbus.heartbeat.type;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum LoggerType {

    HEART_BEAT("heart_beat"), UI("ui");

    private String name;

    private LoggerType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public Logger getLogger() {
        return LoggerFactory.getLogger(name);
    }

    public static LoggerType formStr(String name) {
        if (StringUtils.equals(name, LoggerType.HEART_BEAT.getName())) {
            return LoggerType.HEART_BEAT;
        } else if (StringUtils.equals(name, LoggerType.UI.getName())) {
            return LoggerType.UI;
        } else {
            throw new RuntimeException("根据name: " + name + "不能转换成对应的LoggerType.");
        }
    }

}
