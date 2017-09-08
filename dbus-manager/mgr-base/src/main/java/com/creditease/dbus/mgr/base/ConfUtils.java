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

package com.creditease.dbus.mgr.base;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zhangyf on 17/7/27.
 */
public class ConfUtils {
    public static final String CONF_DIR = SystemUtils.USER_DIR + File.separator + "conf";

    public static final String LOG4J_XML = CONF_DIR + File.separator + "log4j2.xml";

    public static Properties appConf = load("application.properties");

    public static Properties proxyConf = load("proxy.properties");

    public static Properties mybatisConf = load("mybatis.properties");

    public static Properties ctlmsgProducerConf = load("ctlmsg-producer.properties");

    public static Properties load(String properties) {
        InputStream is = null;
        Properties prop = null;
        try {
            File file = new File(SystemUtils.USER_DIR + File.separator + "conf" + File.separator + properties);
            is = new FileInputStream(file);
            prop = new Properties();
            prop.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
        return prop;
    }
}
