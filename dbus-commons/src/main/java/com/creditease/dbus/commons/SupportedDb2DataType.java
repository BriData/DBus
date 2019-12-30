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

public enum SupportedDb2DataType {
    BIGINT, BLOB, CHAR, CHARACTER,
    CHARACTER_VARYING, CHARACTER_LARGE_OBJECT,
    CLOB, DATE, DATE_TIME, DBCLOB, DECFLOAT, DECIMAL, LONG,
    DOUBLE, DOUBLE_PRECISION, GRAPHIC, INTEGER,
    LONG_VARCHAR, LONG_VARGRAPHIC, REAL, SMALLINT,
    TIME, TIMESTAMP, VARCHAR, VARGRAPHIC, XML;

    //BOOL、BOOLEAN在mysql中表示的数据类型都为TINYINT

    private static Map<String, SupportedDb2DataType> map;

    static {
        map = new HashMap<>();
        for (SupportedDb2DataType type : SupportedDb2DataType.values()) {
            map.put(type.name().toUpperCase(), type);
        }
    }

    public static boolean isSupported(String type) {
        type = type.toUpperCase();
        type = type.replace(" ", "_");
        boolean supported = map.containsKey(type);
        if (supported)
            return true;

        // 判断TIMESTAMP(6)类型的type
        return type.contains(TIMESTAMP.name());
    }

    public static SupportedDb2DataType parse(String type) {
        return map.get(type.toUpperCase());
    }

}
