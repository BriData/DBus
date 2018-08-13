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

package com.creditease.dbus.stream.common.appender.bolt.processor;

import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.creditease.dbus.stream.common.Constants.CacheNames;

/**
 * Meta 版本控制器
 * Created by Shrimp on 16/5/23.
 */
public class MetaVerController {

    private static Logger logger = LoggerFactory.getLogger(MetaVerController.class);

    /** 存储version信息到缓存*/
    public static void putVersion(String key, MetaVersion v) {
        ThreadLocalCache.put(CacheNames.META_VERSION_CACHE, key, v);
    }

    /**
     * 通过缓存获取version,如果触发version生成的kafka消息的trailPos比参数传入的pos大,
     * 则需要通过数据库重新获取version
     */
    public static MetaVersion getSuitableVersion(String key, long pos, long offset) {
        MetaVersion version = getVersionFromCache(key);

        // 如果当前使用的trailPos值比消息中的pos值大说明出现重复消费kafka消息的情况,
        // 此时需要找到合适版本的version解析消息
        if (version.getTrailPos() > pos) {
            DataTable table = ThreadLocalCache.get(CacheNames.DATA_TABLES, key);
            version = DBFacadeManager.getDbFacade().queryMetaVersion(table.getId(), pos, offset);
            logger.info("The trail pos[{}] of Cached version is greater than the pos[{}] of message, loading new version[{pos:{}}] from database", version.getTrailPos(), offset, version.getTrailPos());
            putVersion(key, version);
        }
        return version;
    }

    public static MetaVersion getSuitableVersion(String schema, String tableName, long pos, long offset) {
        String key = genKey(schema, tableName);
        return getSuitableVersion(key, pos, offset);
    }

    public static MetaVersion getVersionFromCache(String key) {
        return ThreadLocalCache.get(CacheNames.META_VERSION_CACHE, key);
    }
    public static MetaVersion getVersionFromCache(String schema, String tableName) {
        String key = genKey(schema, tableName);
        return ThreadLocalCache.get(CacheNames.META_VERSION_CACHE, key);
    }
    /** 生成datasource.schema.table格式的key值 */
    public static String genKey(String schema, String tableName) {
        return Utils.join(".", schema, tableName);
    }

    public static void updateVersion(MetaVersion version) throws Exception {
        DBFacadeManager.getDbFacade().updateVersion(version);
    }
}
