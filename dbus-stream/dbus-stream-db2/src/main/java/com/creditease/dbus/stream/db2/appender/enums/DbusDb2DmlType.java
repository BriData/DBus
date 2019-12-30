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


package com.creditease.dbus.stream.db2.appender.enums;

import java.util.HashMap;
import java.util.Map;

public enum DbusDb2DmlType {
    //db2中有几种类型: 1）PT:insert 2) DL:delete 3) UP:update
    PT,
    DL,
    UP;
    public static Map<String, DbusDb2DmlType> map;

    static {
        map = new HashMap<>();
        for (DbusDb2DmlType type : values()) {
            map.put(type.name(), type);
        }
    }

    public static String getDmlType(String t) {
        DbusDb2DmlType type = map.get(t);
        switch (type) {
            case PT:
                return "i";
            case DL:
                return "d";
            case UP:
                return "u";
        }
        return null;
    }
}
