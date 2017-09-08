/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Shrimp on 16/8/19.
 */
public enum DbusDatasourceType {
    ORACLE, MYSQL, JSONLOG, PLAINLOG, UNKNOWN;
    public static Map<String, DbusDatasourceType> map;
    static {
        map = new HashMap<>();
        for (DbusDatasourceType type : values()) {
            map.put(type.name().toLowerCase(), type);
        }
    }

    public static DbusDatasourceType parse(String type) {
        type = type.toLowerCase();
        if (map.containsKey(type)) {
            return map.get(type);
        }
        return DbusDatasourceType.UNKNOWN;
    }

    public static String getDataBaseDriverClass(DbusDatasourceType dbusDatasourceType) {
        switch(dbusDatasourceType) {
            case    ORACLE: return "oracle.jdbc.OracleDriver";
            case    MYSQL: return "com.mysql.jdbc.Driver";
            case    JSONLOG: return "jsonlog from flume";
            case    PLAINLOG: return "plainlog from flume";
            default:
                throw new RuntimeException("Wrong Database type.");
        }
    }
    
    public static void main(String[] args) {
        System.out.println(parse("mysql"));
    }
}
