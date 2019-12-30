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

import java.util.Arrays;
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
    private int countSpaceLine = 0;

    public OrderedProperties(String dataString) {
        formatToMap(dataString);
    }

    public void formatToMap(String dataString) {
        dataString = dataString.replace("\r", "");
        String[] split = dataString.split("\n");
        System.out.println(Arrays.asList(split));
        for (String s : split) {
            if (StringUtils.isBlank(s) || s.startsWith("#") || !s.contains("=")) {
                countSpaceLine++;
                properties.put("null" + countSpaceLine, s);
            } else {
                String[] key_value = s.split("=", 2);
                properties.put(key_value[0], key_value[1]);
            }
        }
        properties.forEach((s, o) -> System.out.println(s + "=" + o));
    }

    public void put(String key, Object value) {
        if (StringUtils.isBlank(key)) {
            properties.put("null" + countSpaceLine, value);
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.startsWith("null")) {
                sb.append(value).append("\n");
            } else {
                sb.append(key).append("=").append(value).append("\n");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }

}
