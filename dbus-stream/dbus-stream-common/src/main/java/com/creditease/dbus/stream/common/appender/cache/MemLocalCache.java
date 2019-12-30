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


package com.creditease.dbus.stream.common.appender.cache;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于内存的简单缓存,暂不设置过期时间或缓存大小等清理机制
 * Created by Shrimp on 16/5/18.
 */
public class MemLocalCache implements LocalCache {
    private ConcurrentMap<String, ConcurrentMap<String, Object>> runtimeCache = new ConcurrentHashMap<>();

    private ConcurrentMap<String, Object> getCache(String cacheName) {
        return runtimeCache.get(cacheName);
    }

    @Override
    public <T> T get(String cacheName, String key) {
        Map<String, Object> cache = getCache(cacheName);
        if (cache == null) {
            return null;
        }
        return (T) cache.get(key);
    }

    @Override
    public void put(String cacheName, String key, Object value) {
        ConcurrentMap<String, Object> cache = getCache(cacheName);
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
            ConcurrentMap<String, Object> oldCache = runtimeCache.putIfAbsent(cacheName, cache);
            if (oldCache != null) {
                cache = oldCache;
            }
        }
        cache.put(key, value);
    }

    @Override
    public void remove(String cacheName, String key) {
        Map<String, Object> cache = getCache(cacheName);
        if (cache != null) {
            cache.remove(key);
        }
    }

    @Override
    public void clear() {
        runtimeCache.clear();
    }


    @Override
    public Map<String, Object> asMap(String cacheName) {
        Map<String, Object> map = getCache(cacheName);
        if (map != null) {
            return ImmutableMap.copyOf(map);
        }
        return null;
    }
}
