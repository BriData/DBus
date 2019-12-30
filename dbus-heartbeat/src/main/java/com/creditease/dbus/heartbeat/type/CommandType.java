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

import com.creditease.dbus.heartbeat.start.HeartBeat;

/**
 * 允许更新zookeeper节点(/Dbus/Heartbeat/Control)的数据值如下:
 * 1:重新加载配置
 * 2:停止心跳,全量拉取检查,心跳检查
 * 3:启动心跳,全量拉取检查,心跳检查
 * 4:停止整个监控进程
 * 5:全量拉取开始
 * 6:全量拉取结束
 *
 * @author Liang.Ma
 * @version 1.0
 */
public enum CommandType {

    RELOAD(1) {
        @Override
        public void exec() {
            target.load();
        }
    },
    STOP(2) {
        @Override
        public void exec() {
        }
    },
    START(3) {
        @Override
        public void exec() {
            target.start();
        }
    },
    DESTORY(4) {
        @Override
        public void exec() {
        }
    },
    FULL_PULLER_BEGIN(5) {
        @Override
        public void exec() {
        }
    },
    FULL_PULLER_END(6) {
        @Override
        public void exec() {
        }
    };

    private int command;

    private static HeartBeat target;

    private CommandType(int command) {
        this.command = command;
    }

    public int getCommand() {
        return command;
    }

    public abstract void exec();

    public static void setTarget(HeartBeat start) {
        target = start;
    }

    public static CommandType fromInt(int intValue) {
        switch (intValue) {
            case 1:
                return CommandType.RELOAD;
            case 2:
                return CommandType.STOP;
            case 3:
                return CommandType.START;
            case 4:
                return CommandType.DESTORY;
            case 5:
                return CommandType.FULL_PULLER_BEGIN;
            case 6:
                return CommandType.FULL_PULLER_END;
            default:
                throw new RuntimeException("Invalid integer value for conversion to commandType");
        }
    }
}
