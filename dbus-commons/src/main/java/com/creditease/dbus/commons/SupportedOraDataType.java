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

import java.util.HashMap;
import java.util.Map;

/**
 * 系统能够支持的oracle数据类型枚举
 * Created by Shrimp on 16/5/26.
 */
public enum SupportedOraDataType {
    BINARY_DOUBLE, BINARY_FLOAT, NUMBER, FLOAT, CHAR, NCHAR, VARCHAR2, NVARCHAR2, DATE, TIMESTAMP, RAW;

    private static Map<String, SupportedOraDataType> map;

    static {
        map = new HashMap<>();
        for (SupportedOraDataType type : SupportedOraDataType.values()) {
            map.put(type.name().toUpperCase(), type);
        }
    }

    /**
     * 判断系统是否支持 type 类型的oracle数据类型
     * 备注:oracle meta信息中会存在 TIMESTAMP(6) 或 TIMESTAMP(3)的值即 type=TIMESTAMP(6)
     *
     * @param type oracle数据类型
     * @return
     */
    public static boolean isSupported(String type) {
        type = type.toUpperCase();
        boolean supported = map.containsKey(type);
        if (supported) return true;

        // 判断TIMESTAMP(6)类型的type
        return type.contains(TIMESTAMP.name());
    }

    public static SupportedOraDataType parse(String type) {
        return map.get(type.toUpperCase());
    }

    /**
     * 判断是否为字符类型
     *
     * @param type 类型名称
     * @return
     */
    public static boolean isCharacterType(String type) {
        SupportedOraDataType dataType = parse(type);
        if (dataType == null) return false;
        switch (dataType) {
            case CHAR:
                return true;
            case NCHAR:
                return true;
            case VARCHAR2:
                return true;
            case NVARCHAR2:
                return true;
            default:
                return false;
        }
    }
}
