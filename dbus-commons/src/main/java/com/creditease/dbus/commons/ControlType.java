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


package com.creditease.dbus.commons;


import java.util.HashMap;
import java.util.Map;

public enum ControlType {
    DISPATCHER_RELOAD_CONFIG,
    DISPATCHER_PAUSE_DATA,
    DISPATCHER_RESUME_DATA,
    DISPATCHER_START_DEBUG,
    DISPATCHER_STOP_DEBUG,
    FULL_DATA_PULL_REQ,
    LOG_PROCESSOR_RELOAD_CONFIG,

    APPENDER_TOPIC_RESUME, // appender 唤醒暂停的consumer
    APPENDER_RELOAD_CONFIG, // appender 重新加载配置
    MONITOR_ALARM, // 监控报警, appender用来停止伪心跳
    G_META_SYNC_WARNING, // meta变更警告事件,G开头代表global消息
    G_MAAS_APPENDER_EVENT, // 通知心跳ALTER, COMMENT-TABLE, COMMENT-COLUMN事件
    COMMON_EMAIL_MESSAGE, // 通用email通知
    KEEPER_PROJECT_EXPIRE, // Dbus-keeper项目到期email通知

    EXTRACTOR_RELOAD_CONF,             // mysql extractor reload config
    MONGO_EXTRACTOR_RELOAD_CONFIG,     // mongo extractor reload config
    ROUTER_RELOAD,                     // router reload
    ROUTER_TOPOLOGY_RERUN,             // router 拓扑重跑
    ROUTER_TOPOLOGY_EFFECT,            // router 拓扑生效
    ROUTER_TOPOLOGY_TABLE_EFFECT,      // router 拓扑表生效
    ROUTER_TOPOLOGY_TABLE_START,       // router 拓扑表启动
    ROUTER_TOPOLOGY_TABLE_STOP,        // router 拓扑表停止

    SINKER_RELOAD_CONFIG,  //sinker重新加载
    SINKER_DRAG_BACK_RUN_AGAIN,  //sinker拖回重跑

    UNKNOWN;

    private static Map<String, ControlType> commands = new HashMap<>();

    static {
        for (ControlType cmd : ControlType.values()) {
            commands.put(cmd.name(), cmd);
        }
    }

    public static ControlType getCommand(String key) {
        if (key == null) {
            return UNKNOWN;
        }

        ControlType command = commands.get(key.toUpperCase());
        if (command == null) {
            return UNKNOWN;
        } else {
            return command;
        }
    }
}
