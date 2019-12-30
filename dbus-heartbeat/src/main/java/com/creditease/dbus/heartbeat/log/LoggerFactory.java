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


package com.creditease.dbus.heartbeat.log;

import com.creditease.dbus.heartbeat.type.LoggerType;
import com.creditease.dbus.heartbeat.util.Constants;
import org.slf4j.Logger;

public class LoggerFactory {

    public static Logger getLogger() {
        String logType = System.getProperty(Constants.SYS_PROPS_LOG_TYPE);
        switch (LoggerType.formStr(logType)) {
            case HEART_BEAT:
                return LoggerType.HEART_BEAT.getLogger();
            case UI:
                return LoggerType.UI.getLogger();
            default:
                throw new RuntimeException("根据logType: " + logType + "不能获得对应的Logger.");
        }
    }

}
