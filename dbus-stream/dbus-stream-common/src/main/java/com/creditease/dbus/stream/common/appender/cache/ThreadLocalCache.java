/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

import com.google.common.base.Joiner;

import java.util.HashSet;
import java.util.Set;

/**
 * 线程本地私有缓存实现
 * Created by Shrimp on 16/5/18.
 */
public class ThreadLocalCache {

    private static CacheProvider provider = new LocalCacheProvider();
    private static ThreadLocal<LocalCache> cachePool = ThreadLocal.withInitial(() -> provider.getCache());
    private static ThreadLocal<Set<String>> blackList = ThreadLocal.withInitial(() -> new HashSet<>());

    public static void put(String cache, String key, Object value) {
        LocalCache localCache = cachePool.get();
        localCache.put(cache, key, value);
    }

    public static <T> T get(String cache, String key) {
        LocalCache localCache = cachePool.get();
        String blackListKey = blackListKey(cache, key);
        try {
            if (blackList.get().contains(blackListKey)) {
                return null;
            }

            T t = localCache.get(cache, key);
            return t;
        } catch (ResultNotFoundException e) {
            blackList.get().add(blackListKey);
            return null;
        }
    }

    public static void remove(String cache, String key) {
        LocalCache localCache = cachePool.get();
        if (localCache != null) {
            localCache.remove(cache, key);
        }
    }

    public static void reload() {
        cachePool.remove();
        blackList.remove();
    }

    private static String blackListKey(String... parts) {
        return Joiner.on("-").join(parts);
    }
}
