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

import com.creditease.dbus.commons.exception.UnintializedException;
import com.creditease.dbus.stream.common.Constants.CacheNames;
import com.creditease.dbus.stream.common.appender.db.DruidDataSourceProvider;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class DbusCache {
    private static Logger logger = LoggerFactory.getLogger(DbusCache.class);
    private static DbusCacheLoader loader;
    private static ConcurrentHashMap<String, Object> cache;
    private static AtomicBoolean initialized = new AtomicBoolean(false);

    static {
        cache = new ConcurrentHashMap<>();
    }

    private static void initialize() {
        try {
            Properties properties = (Properties) GlobalCache.getCache(GlobalCache.Const.DBUS_MANAGER_CONF);
            DruidDataSourceProvider provider = new DruidDataSourceProvider(properties);
            loader = new DbusCacheLoader(provider.provideDataSource());
        } catch (Exception e) {
            logger.error("[dispatcher] DbusCache initialize error", e);
            throw new UnintializedException("DbusCache initialize error", e);
        }
    }

    private static void checkInit() {
        if (!initialized.get()) {
            synchronized (DBFacadeManager.class) {
                if (!initialized.get()) {
                    initialize();
                    logger.info("[dispatcher]  DbusCache initialized");
                }
            }
            initialized.set(true);
        }
    }

    public static Object getCache(String type, String... params) {
        Object result = cache.get(type);
        if (result == null) {
            result = load(type, params);
            cache.put(type, result);
        }
        return result;
    }

    private static Object load(String type, String... params) {
        checkInit();
        Object result = null;
        long dsId = Utils.getDatasource().getId();
        try {
            switch (type) {
                case CacheNames.DATA_TABLES:
                    logger.info("[dispatcher] Query all table from database with dsId:{}", dsId);
                    result = loader.queryDataTable(dsId);
                    break;
                default:
                    logger.warn("[dispatcher] Unsupported type[{}] of CacheLoader", type);
            }
        } catch (Exception e) {
            logger.error("[dispatcher] Fail to get Object from database with parameter:{type:'{}',param:'{}'", type, dsId, e);
        }
        return result;
    }

}
