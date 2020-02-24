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

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/10/25
 */
public class OrderedProperties {
    private final long serialVersionUID = -4627607243846121965L;

    private final LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
    private int emptyCount = 0;
    private static final String emptyFlag = "{NULL}";

    public OrderedProperties(String dataString) {
        formatToMap(dataString);
    }

    public void formatToMap(String dataString) {
        dataString = StringUtils.replace(dataString, "\r", "");
        for (String line : StringUtils.split(dataString, "\n")) {
            line = line.trim();
            if (StringUtils.isBlank(line) || line.startsWith("#") || line.startsWith("!")) {
                emptyCount++;
                properties.put(emptyFlag + emptyCount, line);
            } else if (!line.contains("=") && !line.contains(":")) {
                emptyCount++;
                properties.put(emptyFlag + emptyCount, line);
            } else {
                String escape = getEscape(line);
                String[] kv = StringUtils.split(line, escape, 2);
                if (kv.length == 2) {
                    properties.put(kv[0], kv[1]);
                } else {
                    properties.put(kv[0], null);
                }
            }
        }
    }

    public void put(String key, Object value) {
        if (StringUtils.isBlank(key)) {
            properties.put(emptyFlag + emptyCount, value);
        } else {
            properties.put(key, value);
        }
    }

    public Object get(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        } else {
            return properties.get(key);
        }
    }

    public Object remove(String key) {
        if (StringUtils.isBlank(key)) {
            throw new NullPointerException();
        } else {
            return properties.remove(key);
        }
    }

    private static String getEscape(String line) {
        String escape = "=";
        if (line.contains(":") && (line.indexOf("=") > line.indexOf(":"))) {
            escape = ":";
        }
        return escape;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.startsWith(emptyFlag)) {
                sb.append(value).append("\n");
            } else if (value == null) {
                sb.append(key).append("=").append("\n");
            } else {
                sb.append(key).append("=").append(value).append("\n");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }

}
