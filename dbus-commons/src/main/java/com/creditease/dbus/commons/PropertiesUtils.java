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

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * 配置文件读取工具
 * Created by Shrimp on 16/5/14.
 */
public class PropertiesUtils {
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * create a Properties object use the config file
     *
     * @param configFile a file path
     * @return Properties object
     * @throws Exception
     */
    public static Properties getProps(String configFile) throws IOException {
        //InputStream is = getClass().getClassLoader().getResourceAsStream(configFile);
        InputStream is = null;
        try {
            is = Resources.getResource(configFile).openStream();

            Properties props = new Properties();
            props.load(is);
            return props;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    public static Properties copy(Properties props) {
        if (props == null) return null;
        Properties dest = new Properties();
        Enumeration<?> keys = props.propertyNames();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            dest.setProperty(key, (String) props.get(key));
        }
        return dest;
    }
}
