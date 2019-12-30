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

import java.nio.charset.Charset;

/**
 * Created by zhangyf on 16/11/10.
 */
@Deprecated
public abstract class HashStrategy implements EncodeStrategy {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public Object encode(DbusMessage.Field field, Object value, EncodeColumn col) {
        if (value == null) return null;
        switch (field.dataType()) {
            case DECIMAL:
                return String.format("%.1f", hash(value, col).asLong() / 3.0);
            case LONG:
                return hash(value, col).asLong();
            case INT:
                return hash(value, col).asInt();
            case DOUBLE:
                return hash(value, col).asLong() / 2.0d;
            case FLOAT:
                return hash(value, col).asInt() / 2.0f;
            case DATE:
                return new DefaultValueStrategy().encode(field, value, col);
            case DATETIME:
                return new DefaultValueStrategy().encode(field, value, col);
            default:
                return hash(value, col).toString();
        }
    }

    protected abstract HashCode hash(Object str, EncodeColumn col);
}
