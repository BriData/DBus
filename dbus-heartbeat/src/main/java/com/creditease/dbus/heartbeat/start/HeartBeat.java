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

import com.creditease.dbus.heartbeat.handler.IHandler;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import org.apache.commons.lang.SystemUtils;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public abstract class HeartBeat {

    static {
        // 设置log4j.xml配置文件位置
        //  	String logConf = DOMConfigurator.class.getClassLoader().getResource("log4j.xml").toString();
        //  	System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG,logConf);
        String userDir = null;
        try {
            userDir = new String(SystemUtils.USER_DIR.replaceAll("\\\\", "/").getBytes("UTF-8"));
            userDir = URLEncoder.encode(userDir, "UTF-8");

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG,"file:" + userDir + "/conf/log4j.xml");
//        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG,"file:" + userDir + "/conf_creditease_setting/log4j.xml");
        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG, "file:" + SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/conf/log4j.xml");
    }

    protected List<IHandler> handlers = new ArrayList<IHandler>();

    protected void run() {
        unregister();
        register();
        load();
    }

    public abstract void start();

    public void load() {
        for (IHandler handler : handlers) {
            LoggerFactory.getLogger().info("执行:{}", handler.getClass().getSimpleName());
            handler.process();
        }
    }

    protected void registerHandler(IHandler handler) {
        handlers.add(handler);
        LoggerFactory.getLogger().info("注册:{}", handler.getClass().getSimpleName());
    }

    protected abstract void register();


    protected void unregister() {
        handlers.clear();
    }
}
