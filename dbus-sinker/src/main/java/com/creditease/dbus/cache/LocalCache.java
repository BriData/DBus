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


package com.creditease.dbus.cache;

import com.creditease.dbus.bean.HdfsOutputStreamInfo;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LocalCache {

    private static ThreadLocal<ConcurrentHashMap<String, HdfsOutputStreamInfo>> local = ThreadLocal.withInitial(() -> new ConcurrentHashMap<>());

    public static void put(String key, HdfsOutputStreamInfo outputStreamInfo) {
        ConcurrentHashMap<String, HdfsOutputStreamInfo> map = local.get();
        map.put(key, outputStreamInfo);
        local.set(map);
    }

    public static HdfsOutputStreamInfo get(String key) {
        return local.get().get(key);
    }

    public static void remove(String key) {
        ConcurrentHashMap<String, HdfsOutputStreamInfo> map = local.get();
        map.remove(key);
        local.set(map);
    }

    public static Set<String> getAllKeys() {
        return local.get().keySet();
    }
}
