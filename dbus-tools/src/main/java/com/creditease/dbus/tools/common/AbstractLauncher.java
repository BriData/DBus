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


package com.creditease.dbus.tools.common;

import com.creditease.dbus.commons.Constants;
import org.apache.commons.lang.SystemUtils;

import java.net.URLEncoder;

public class AbstractLauncher {


    static {
        // 设置log4j.xml配置文件位置
        String userDir = null;
        try {
            userDir = new String(SystemUtils.USER_DIR.replaceAll("\\\\", "/").getBytes("UTF-8"));
            userDir = URLEncoder.encode(userDir, "UTF-8");
            System.out.println("UserDir=" + userDir);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG, "file:" + userDir + "/conf/log4j.xml");
//    	System.setProperty(Constants.SYS_PROPS_LOG4J2_CONFIG,"file:" + userDir + "/conf/log4j2.xml");
        
    /* // 获取存储log的基准path
        String logBasePath = System.getProperty(
                Constants.SYS_PROPS_LOG_BASE_PATH, SystemUtils.USER_DIR);
        // 设置log4j日志保存目录
        System.setProperty(Constants.SYS_PROPS_LOG_DIR, 
                logBasePath.replaceAll("\\\\", "/") + "/logs");*/
    }
}
