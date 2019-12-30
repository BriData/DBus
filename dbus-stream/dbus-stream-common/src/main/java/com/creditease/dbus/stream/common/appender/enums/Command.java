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


package com.creditease.dbus.stream.common.appender.enums;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.exception.UnintializedException;
import com.creditease.dbus.stream.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 命令枚举
 * Created by Shrimp on 16/6/13.
 */
public enum Command {
    UNKNOWN_CMD,
    PAUSE_APPENDER_DATA_TOPIC, // 暂停appender所有data topic
    RESUME_APPENDER, // 继续appender所有data topic
    FULL_DATA_PULL_REQ, // 拉全量枚举
    HEART_BEAT, // 心跳
    SPLIT_ACK,
    META_SYNC, // 同步meta信息
    APPENDER_TOPIC_RESUME, // 继续某个topic
    AVRO_SCHEMA,
    MONITOR_ALARM, // 监控报警,停止伪心跳
    DATA_INCREMENT_TERMINATION, // 停止发送消息到kafka后发给wormhole的termination消息
    META_EVENT_WARNING, // meta变更兼容性事件警告命令
    APPENDER_RELOAD_CONFIG; // 重新加载配置

    private static Logger logger = LoggerFactory.getLogger(Command.class);

    private static boolean initialized = false;
    private static Map<String, Command> cmds = new HashMap<>();
    private static Map<String, Command> nameCmds = new HashMap<>();

    static {
        for (Command command : Command.values()) {
            nameCmds.put(command.name().toLowerCase(), command);
        }
    }

    public static void initialize() {
        cmds.put($(Constants.ConfigureKey.FULLDATA_REQUEST_SRC), FULL_DATA_PULL_REQ);
        cmds.put($(Constants.ConfigureKey.HEARTBEAT_SRC), HEART_BEAT);
        cmds.put($(Constants.ConfigureKey.META_EVENT_SRC), META_SYNC);
        logger.info("Command initialize {}", cmds.toString());
        initialized = true;
    }

    public static Command parse(String cmd) {
        if (!initialized) {
            throw new UnintializedException("Command has not initialized!");
        }
        cmd = cmd.toLowerCase();
        if (cmds.containsKey(cmd)) {
            return cmds.get(cmd);
        } else if (nameCmds.containsKey(cmd)) {
            return nameCmds.get(cmd);
        }
        return UNKNOWN_CMD;
    }

    private static String $(String key) {
        return PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, key).toLowerCase();
    }

    public static void main(String[] args) {
        System.out.println(Command.FULL_DATA_PULL_REQ);
    }
}
