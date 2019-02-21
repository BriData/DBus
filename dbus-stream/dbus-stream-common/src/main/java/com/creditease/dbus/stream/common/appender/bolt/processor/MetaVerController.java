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

import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.creditease.dbus.stream.common.Constants.CacheNames;

/**
 * Meta 版本控制器
 * Created by Shrimp on 16/5/23.
 */
public class MetaVerController {

    private static Logger logger = LoggerFactory.getLogger(MetaVerController.class);

    /**
     * 存储version信息到缓存
     */
    public static void putVersion(String key, MetaVersion v) {
        ThreadLocalCache.put(CacheNames.META_VERSION_CACHE, key, v);
    }

    /**
     * 通过缓存获取version,如果触发version生成的kafka消息的trailPos比参数传入的pos大,
     * 则需要通过数据库重新获取version
     */
    public static MetaVersion getSuitableVersion(String key, long pos, long offset) {
        MetaVersion version = getVersionFromCache(key);

        if (version == null) {
            String schema = StringUtils.substringBefore(key, ".");
            String table = StringUtils.substringAfter(key, ".");
            DataTable t = DBFacadeManager.getDbFacade().queryDataTable(Utils.getDatasource().getId(), schema, table);
            if (t == null) {
                logger.warn("table{} not found.", key);
                return null;
            }
            version = DBFacadeManager.getDbFacade().queryMetaVersion(t.getId(), pos, offset);
            if (version != null) {
                putVersion(key, version);
            } else {
                logger.warn("version not found by key: {}", key);
            }
            return version;
        }

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


    //从数据库中查询version
    public static MetaVersion getMetaVersionFromDB(String schema, String tableName, String pos, long time) {
        String key = genKey(schema, tableName);
        MetaVersion version = null;

        MetaVersion latestV = getLatestMetaVersionFromDB(schema, tableName);
        if (time >= latestV.getOffset() && Long.parseLong(pos) != latestV.getTrailPos()) {
            return null;
        } else if (time >= latestV.getOffset() && Long.parseLong(pos) == latestV.getTrailPos()) {
            clearAllVersionsFromCache(schema, tableName);
            ThreadLocalCache.put(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, genSchemaIdKey(schema, tableName, pos), latestV);
            return latestV;
        } else {
            DataTable table = ThreadLocalCache.get(CacheNames.DATA_TABLES, key);
            //查询精确的版本，offset：某个表结构变更事件的时间点, 实际上是两条不同的数据导致变更，而采用的后一条数据的日志时间点
            version = DBFacadeManager.getDbFacade().queryMetaVersionFromDB(table.getId(), Long.parseLong(pos), time);
            if (version != null) {
                clearAllVersionsFromCache(schema, tableName);
                ThreadLocalCache.put(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, genSchemaIdKey(schema, tableName, pos), version);
            } else {
                //当重读位置不是变更记录的点时（大多数情况应该都是如此），需要去数据库中查询相对应的version
                version = DBFacadeManager.getDbFacade().queryMetaVersionByTime(table.getId(), Long.parseLong(pos), time);
                if (version == null) {
                    logger.info("出现数据的event_time小于当前版本变更时最早的时间，但两者的pos值一致！");
                    logger.info("最新版本的event_time： {}，pos：{}，该条数据的event_time：{}，pos:{}", latestV.getOffset(), latestV.getTrailPos(), time, pos);
                    //如果出现此情况，则应该根据pos值去数据库中查询该pos值对应的最新版本的数据
                    //重读数据的时候不应该走到这个位置
                    version = DBFacadeManager.getDbFacade().queryMetaVersion(table.getId(), Long.parseLong(pos));
//                    logger.info("version should not be null , please check data! schema:{}, table:{}, pos:{}, time:{}", schema, tableName, pos, time);
                }
                clearAllVersionsFromCache(schema, tableName);
                ThreadLocalCache.put(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, genSchemaIdKey(schema, tableName, pos), version);

            }
            return version;
        }

    }

    //从数据库中查询最近的version
    public static MetaVersion getLatestMetaVersionFromDB(String schema, String tableName) {
        String key = genKey(schema, tableName);

        DataTable table = ThreadLocalCache.get(CacheNames.DATA_TABLES, key);
        MetaVersion version = DBFacadeManager.getDbFacade().queryLatestMetaVersion(table.getId());

        logger.info("get latest version info from mgr db, version: {} ", version);

        return version;
    }

    //从数据库中查询version表中是否存在该schema id的版本
    public static MetaVersion putMetaVersionCache(MetaVersion version) {
        clearAllVersionsFromCache(version.getSchema(), version.getTable());

        if (version != null) {
            ThreadLocalCache.put(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, genSchemaIdKey(version.getSchema(), version.getTable(), String.valueOf(version.getTrailPos())), version);
        }

        return version;
    }


    public static MetaVersion getMetaVersionFromCache(String schema, String tableName, String pos, Long time) {
        String key = genSchemaIdKey(schema, tableName, pos);
        MetaVersion v = ThreadLocalCache.get(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, key);

        if (v != null) {
            if (time < v.getOffset()) {
                return null;
            }
        }
        return v;
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

    /**
     * 生成datasource.schema.table格式的key值
     */
    public static String genKey(String schema, String tableName) {
        return Utils.join(".", schema, tableName);
    }

    public static String genSchemaIdKey(String schema, String tableName, String schemaId) {
        return Utils.join(".", schema, tableName, schemaId);
    }

    public static void updateVersion(MetaVersion version) throws Exception {
        DBFacadeManager.getDbFacade().updateVersion(version);
    }

    public static void clearAllVersionsFromCache(String schema, String tableName) {
        String key = genKey(schema, tableName);

        DataTable table = ThreadLocalCache.get(CacheNames.DATA_TABLES, key);
        List<Long> allPos = DBFacadeManager.getDbFacade().queryAllPos(table.getId());

        for (int i = 0; i < allPos.size(); i++) {
            ThreadLocalCache.remove(CacheNames.TABLE_CURRENT_SCHEMA_CACHE, genSchemaIdKey(schema, tableName, String.valueOf(allPos.get(i))));
        }
    }

}
