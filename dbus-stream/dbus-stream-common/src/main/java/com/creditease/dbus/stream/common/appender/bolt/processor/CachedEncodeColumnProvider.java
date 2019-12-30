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


package com.creditease.dbus.stream.common.appender.bolt.processor;

import com.creditease.dbus.msgencoder.EncodeColumn;
import com.creditease.dbus.msgencoder.EncodeColumnProvider;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;

import java.util.List;

/**
 * Created by zhangyf on 16/11/10.
 */
public class CachedEncodeColumnProvider implements EncodeColumnProvider {
    private long tableId;

    public CachedEncodeColumnProvider(long tableId) {
        this.tableId = tableId;
    }

    @Override
    public List<EncodeColumn> getColumns() {
        return ThreadLocalCache.get(Constants.CacheNames.TAB_ENCODE_FIELDS, this.tableId + "");
    }
}
