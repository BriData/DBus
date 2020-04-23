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


package com.creditease.dbus.stream.common.dispatcher;

import com.creditease.dbus.commons.exception.InitializationException;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


public class GlobalCache {
    private static Logger logger = LoggerFactory.getLogger(GlobalCache.class);
    private static String datasource;
    private static String zkServers;
    private static ConcurrentHashMap<String, Object> cache;

    public static class Const {
        public static final String DBUS_MANAGER_CONF = "dbusManagerConf";
        public static final String PHYSICAL_TABLE_REGEX = "physicalTableRegex";
    }

    static {
        cache = new ConcurrentHashMap<>();
    }

    public static synchronized void initialize(String ds, String zk) {
        if (datasource == null || zkServers == null) {
            if (Strings.isNullOrEmpty(ds)) {
                throw new IllegalArgumentException("Initial parameter can not be null or empty string");
            }
            datasource = ds;
            zkServers = zk;
        }
    }

    public static String getZkServers() {
        checkInit();
        try {
            return zkServers;
        } catch (Exception e) {
            logger.error("Encounter an error while getting zkServers!!!", e);
            throw new RuntimeException(e);
        }
    }

    public static String getDatasource() {
        checkInit();
        try {
            return datasource;
        } catch (Exception e) {
            logger.error("Encounter an error while getting datasource!!!", e);
            throw new RuntimeException(e);
        }
    }

    public static Object getCache(String key) {
        checkInit();
        try {
            return cache.get(key);
        } catch (Exception e) {
            logger.error(String.format("Encounter an error while getting cache %s!!!", key), e);
            throw new RuntimeException(e);
        }
    }

    public static void setCache(String key, Object value) {
        checkInit();
        try {
            cache.put(key, value);
        } catch (Exception e) {
            logger.error(String.format("Encounter an error while setting cache  %s!!!", key), e);
            throw new RuntimeException(e);
        }
    }

    public static void refreshCache() {
        cache.clear();
    }

    private static void checkInit() {
        if (datasource == null || zkServers == null) {
            throw new InitializationException("Please initialize GlobalCache with calling GlobalCache.initialize(String datasource)");
        }
    }

}
