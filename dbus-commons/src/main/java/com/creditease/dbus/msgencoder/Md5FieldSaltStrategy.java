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

import com.google.common.hash.Hashing;

/**
 * Created by zhangyf on 16/12/8.
 */
@Deprecated
public class Md5FieldSaltStrategy extends HashStrategy {
    private String saltValue;

    @Override
    protected HashCode hash(Object value, EncodeColumn col) {
        return new DBusHashCode(Hashing.md5().hashString(value.toString() + saltValue, UTF8));
    }

    @Override
    public void set(Object params) {
        this.saltValue = params != null ? params.toString() : "";
    }
}
