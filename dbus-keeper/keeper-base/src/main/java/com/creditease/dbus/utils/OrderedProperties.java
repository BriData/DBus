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
    private static final long serialVersionUID = -4627607243846121965L;

    private static final LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
    private static int countSpaceLine = 0;

    public OrderedProperties(String dataString) {
        formatToMap(dataString);
    }

    public OrderedProperties() {
    }

    public static void formatToMap(String dataString) {
        dataString = dataString.replace("\r", "");
        String[] split = dataString.split("\n");
        for (String s : split) {
            if (StringUtils.isBlank(s) || s.startsWith("#")) {
                countSpaceLine++;
                properties.put("null" + countSpaceLine, s);
            } else {
                String[] key_value = s.split("=", 2);
                properties.put(key_value[0], key_value[1]);
            }
        }
    }

    public static void put(String key, Object value) {
        if (StringUtils.isBlank(key)) {
            properties.put("null" + countSpaceLine, value);
        } else {
            properties.put(key, value);
        }
    }

    public static Object get(String key) {
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
