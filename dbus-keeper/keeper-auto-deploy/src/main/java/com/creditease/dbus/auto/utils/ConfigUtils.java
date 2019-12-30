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


package com.creditease.dbus.auto.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class ConfigUtils {
    public static Properties loadConfig() throws Exception {
        FileInputStream in = null;
        try {
            System.out.println("加载config.properties配置文件中...");
            Properties pro = new Properties();
            in = new FileInputStream("../conf/config.properties");
            pro.load(in);
            in.close();
            return pro;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
