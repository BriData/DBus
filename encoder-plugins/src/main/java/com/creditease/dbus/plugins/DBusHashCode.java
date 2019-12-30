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


package com.creditease.dbus.plugins;

/**
 * Created by zhangyf on 16/11/18.
 */
public class DBusHashCode implements HashCode {
    private com.google.common.hash.HashCode hashCode;

    public DBusHashCode(com.google.common.hash.HashCode hashCode) {
        this.hashCode = hashCode;
    }

    @Override
    public long asLong() {
        return hashCode.asLong();
    }

    @Override
    public int asInt() {
        return hashCode.asInt();
    }

    @Override
    public String toString() {
        return hashCode.toString();
    }
}
