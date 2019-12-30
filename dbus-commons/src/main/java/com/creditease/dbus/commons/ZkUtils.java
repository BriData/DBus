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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZkUtils {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public static Properties getProperties(String zkServers, String path) throws Exception {
        ZkService zkService = null;
        try {
            zkService = new ZkService(zkServers);

            return zkService.getProperties(path);
        } finally {
            if (zkService != null) {
                zkService.close();
            }
        }
    }

    public static String getData(String zkServers, String path) throws Exception {
        ZkService zkService = null;
        try {
            zkService = new ZkService(zkServers);

            byte[] data = zkService.getData(path);
            return new String(data, "UTF-8");
        } finally {
            if (zkService != null) {
                zkService.close();
            }
        }

    }


    public static void setProperties(String zkServers, String path, Properties props) throws Exception {
        ZkService zkService = null;
        try {
            zkService = new ZkService(zkServers);

            zkService.setProperties(path, props);
        } finally {
            if (zkService != null) {
                zkService.close();
            }
        }
    }
}
