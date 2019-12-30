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


package com.creditease.dbus.msgencoder;

import com.creditease.dbus.commons.DbusMessage;

/**
 * Created by zhangyf on 16/11/10.
 */
@Deprecated
public class DefaultValueStrategy implements EncodeStrategy {
    @Override
    public Object encode(DbusMessage.Field field, Object value, EncodeColumn col) {
        if (value == null) return null;
        switch (field.dataType()) {
            case DECIMAL:
                return "0";
            case LONG:
                return "0";
            case INT:
                return 0;
            case DOUBLE:
                return 0.0;
            case FLOAT:
                return 0.0f;
            case DATE:
                return "1970-01-01";
            case DATETIME:
                return "1970-01-01 08:00:01.000";
            default:
                return "*";
        }
    }
}
