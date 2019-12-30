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


package com.creditease.dbus.utils;


import org.apache.commons.lang3.SystemUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfUtils {
    private static String user_dir_parent = SystemUtils.USER_DIR.replaceAll("\\\\", "/");

    public static Properties loadBasicProperties() throws IOException {
        return getProps("basic.properties");
    }

    public static String toConfPath(String fileName) {
        String path = String.format("%s/../conf/%s", getParentPath(), fileName);
        return path;
    }

    public static String getParentPath() {
        return user_dir_parent;
    }

    public static Properties getProps(String configFile) throws IOException {
        FileInputStream fis = null;

        try {
            String fullPath = toConfPath(configFile);
            fis = new FileInputStream(fullPath);

            Properties props = new Properties();
            props.load(fis);
            return props;
        } finally {
            if (fis != null)
                fis.close();
        }
    }

    public static byte[] toByteArray(String configFile) throws IOException {
        FileInputStream fis = null;

        try {
            String fullPath = toConfPath(configFile);
            fis = new FileInputStream(fullPath);

            byte[] data = new byte[fis.available()];
            fis.read(data);

            return data;
        } finally {
            if (fis != null)
                fis.close();
        }
    }

    public static String toString(String configFile) throws IOException {
        FileInputStream fis = null;
        try {
            String fullPath = toConfPath(configFile);
            fis = new FileInputStream(fullPath);

            byte[] data = new byte[fis.available()];
            fis.read(data);

            return new String(data, "utf-8");
        } finally {
            if (fis != null)
                fis.close();
        }
    }
}
