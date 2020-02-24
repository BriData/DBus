package com.creditease.dbus.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ParamUtils {

    public static void putNotNull(Map<String, Object> map, String key, Object value) {
        if (key == null || value == null || map == null) {
            return;
        }
        if (value instanceof String && StringUtils.isNotBlank((String) value)) {
            map.put(key, value);
        }
    }
}
