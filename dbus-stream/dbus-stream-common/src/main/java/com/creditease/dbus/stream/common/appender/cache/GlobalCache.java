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

import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.appender.bean.DbusDatasource;
import com.creditease.dbus.stream.common.appender.bean.NameAliasMapping;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

/**
 * 全局的缓存,和ThreadLocalCache相互辅助使用,需要在调用前初始化
 * Created by Shrimp on 16/8/17.
 */
public class GlobalCache {
    private static Logger logger = LoggerFactory.getLogger(GlobalCache.class);
    private static String datasource;
    private static Cache<String, Object> cache;

    private static class Const {
        public static final String DATASOURCE = "datasource";
        public static final String NAME_ALIAS_MAPPING = "nameAliasMapping";
    }

    static {
        cache = CacheBuilder.newBuilder().build();
    }

    public static synchronized void initialize(String ds) {
        if (datasource == null) {
            if (Strings.isNullOrEmpty(ds)) {
                throw new IllegalArgumentException("Initial parameter can not be null or empty string");
            }
            datasource = ds;
        }
    }

    public static DbusDatasource getDatasource() {
        checkInit();
        try {
            DbusDatasource ds = (DbusDatasource) cache.get(Const.DATASOURCE, () -> {
                Map<String, Object> map = DBFacadeManager.getDbFacade().queryDatasource(datasource);
                return convert(map);
            });
            return ds;
        } catch (Exception e) {
            logger.error("Encounter an error while getting datasource!!!", e);
            throw new RuntimeException(e);
        }
    }

    public static NameAliasMapping getNameAliasMapping() {
        checkInit();
        try {
            return (NameAliasMapping) cache.get(Const.NAME_ALIAS_MAPPING, () -> {
                        DbusDatasource ds = getDatasource();
                        NameAliasMapping m = DBFacadeManager.getDbFacade().getNameMapping(ds.getId());
                        if (m == null) {
                            m = new NameAliasMapping();
                            m.setId(0);
                            m.setId(ds.getId());
                            m.setType(2);
                            m.setName(ds.getDsName());
                            m.setAlias(ds.getDsName());
                            m.setTs(new Timestamp(System.currentTimeMillis()));
                            logger.info("Name alias mapping not found, default alias[{}] used.", m.getAlias());
                        } else {
                            logger.warn("DBus data source alias found. Alias[{}] will be used in UMS while building namespace.", m.getAlias());
                        }
                        return m;
                    }
            );
        } catch (Exception e) {
            logger.error("Encounter an error while getting name alias mapping!!!", e);
            throw new RuntimeException(e);
        }

    }

    public static DbusDatasourceType getDatasourceType() {
        DbusDatasource ds = getDatasource();
        if (ds != null) {
            return DbusDatasourceType.parse(ds.getDsType());
        }
        throw new RuntimeException("Datasource not found!!!");
    }

    public static void refreshCache() {
        cache.invalidateAll();
    }

    private static void checkInit() {
        if (datasource == null) {
            throw new InitializationException("Please initialize GlobalCache with calling GlobalCache.initialize(String datasource)");
        }
    }

    private static DbusDatasource convert(Map<String, Object> map) {
        DbusDatasource ds = new DbusDatasource();
        ds.setId((long) map.get("id"));
        ds.setDsName(map.get("ds_name").toString());
        ds.setDsType(map.get("ds_type").toString());
        ds.setDsDesc(map.get("ds_desc").toString());
        ds.setMasterUrl(map.get("master_url").toString());
        ds.setSlaveUrl(map.get("slave_url").toString());
        ds.setTs((Timestamp) map.get("update_time"));
        ds.setDbusUser(map.get("dbus_user").toString());
        ds.setDbusPwd(map.get("dbus_pwd").toString());
        ds.setSchemaTopic(map.get("schema_topic").toString());
        ds.setControlTopic(map.get("ctrl_topic").toString());
        ds.setSplitTopic(map.get("split_topic").toString());
        ds.setTopic(map.get("topic").toString());
        return ds;
    }
}
