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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 以Guava Cache为基础实现的本地缓存
 * Created by Shrimp on 16/8/17.
 */
public class GuavaLocalCache implements LocalCache {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final int MAX_SIZE = 1024;
    private static final int EXPIRE_MINUTE = 10;

    private LocalCacheLoader cacheLoader;

    private ConcurrentMap<String, Cache<String, Object>> container;

    public GuavaLocalCache() {
        container = new ConcurrentHashMap<>();
        cacheLoader = new DbusCacheLoader();
    }

    private Cache<String, Object> getCache(String cacheName) {
        Cache<String, Object> cache = container.get(cacheName);
        if (cache == null) {
            cache = buildCache();
            Cache<String, Object> oldCache = container.putIfAbsent(cacheName, cache);
            if (oldCache != null) {
                cache = oldCache;
            }
        }
        return cache;
    }

    private Cache<String, Object> buildCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(MAX_SIZE)
                .expireAfterAccess(EXPIRE_MINUTE, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public <T> T get(final String cacheName, final String key) {
        Cache<String, Object> cache = getCache(cacheName);
        try {
            return (T) cache.get(key, () -> {
                Object value = cacheLoader.load(cacheName, key);
                if (value == null) {
                    throw new ResultNotFoundException();
                }
                return value;
            });
        } catch (ExecutionException e) {
            logger.warn("Exception was throw when get value from cache[{}] with key[{}]!", cacheName, key);
        } catch (UncheckedExecutionException e) {
            if (ResultNotFoundException.class.isInstance(e.getCause())) {
                logger.warn("Result not found from database, cache name:{}, key:{}", cacheName, key);
                throw new ResultNotFoundException();
            }
            throw e;
        }
        return null;
    }

    @Override
    public void put(String cacheName, String key, Object value) {
        Cache<String, Object> cache = getCache(cacheName);
        cache.put(key, value);
    }

    @Override
    public void clear() {
        container.clear();
    }

    @Override
    public void remove(String cacheName, String key) {
        Cache<String, Object> cache = getCache(cacheName);
        cache.invalidate(key);
    }

    @Override
    public Map<String, Object> asMap(String cacheName) {
        Cache<String, Object> cache = container.get(cacheName);
        if (cache == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> map = cache.asMap();
        if (map != null) {
            return ImmutableMap.copyOf(map);
        }
        return Collections.emptyMap();
    }
}
