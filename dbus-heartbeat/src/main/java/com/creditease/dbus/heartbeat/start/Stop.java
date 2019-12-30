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


package com.creditease.dbus.heartbeat.start;

import com.creditease.dbus.heartbeat.handler.impl.DestoryHeartBeatHandler;
import com.creditease.dbus.heartbeat.handler.impl.LoadFileConfigHandler;
import com.creditease.dbus.heartbeat.handler.impl.LoadZkConfigHandler;
import com.creditease.dbus.heartbeat.type.LoggerType;
import com.creditease.dbus.heartbeat.util.Constants;
import org.apache.commons.lang.SystemUtils;

public class Stop extends HeartBeat {

    static {
        // 获取存储log的基准path
        String logBasePath = System.getProperty(
                Constants.SYS_PROPS_LOG_BASE_PATH, SystemUtils.USER_DIR);
        // 设置log4j日志保存目录
        System.setProperty(Constants.SYS_PROPS_LOG_HOME,
                logBasePath.replaceAll("\\\\", "/") + "/logs/heartbeat");
        // 设置日志类型
        System.setProperty(Constants.SYS_PROPS_LOG_TYPE, LoggerType.HEART_BEAT.getName());
    }

    public static void main(String[] args) {
        HeartBeat start = new Stop();
        start.run();
    }


    @Override
    protected void register() {
        // 注册加载jdbc.properties和zk.properties配置信息handler
        registerHandler(new LoadFileConfigHandler());
        // 注册加载zk配置信息
        registerHandler(new LoadZkConfigHandler());
        // 注册加载关闭心跳handler
        registerHandler(new DestoryHeartBeatHandler());
    }

    public void start() {
        //do nothing
    }
}
