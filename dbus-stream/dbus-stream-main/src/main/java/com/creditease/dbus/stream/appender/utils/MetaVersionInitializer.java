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

package com.creditease.dbus.stream.appender.utils;

import avro.shaded.com.google.common.collect.Sets;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bean.TabSchema;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherException;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 在这里初始化meta version并不合适,将来需要整合到web管理端去完成
 * 在添加数据源的时候应该把meta version初始化到mysql中
 * Created by Shrimp on 16/8/17.
 */
public class MetaVersionInitializer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public void initialize(DbusDatasourceType dsType) throws Exception {
        // 通过数据库加载schema列表
        List<TabSchema> schemaList = DBFacadeManager.getDbFacade().queryDataSchemas(Utils.getDatasource().getId());

        // 过滤掉没有配置的schema
        filter(schemaList);
        if(DbusDatasourceType.ORACLE == dsType) {

            // 支持reload操作
            MetaFetcherManager.reset();

            for (TabSchema schema : schemaList) {
                reloadVersions(DBFacadeManager.getDbFacade().queryDataTables(schema.getId()));
            }
        } else if(DbusDatasourceType.MYSQL == dsType){
            for (TabSchema schema : schemaList) {
                List<DataTable> dts = DBFacadeManager.getDbFacade().queryDataTables(schema.getId());
                for(DataTable dt : dts){
                    MetaVersion metaVer = DBFacadeManager.getDbFacade().queryMetaVersion(dt.getDsId(), dt.getSchema(), dt.getTableName());
                    if(metaVer==null){
                        MetaVersion ver = new MetaVersion();
                        ver.setTableId(dt.getId());
                        ver.setDsId(dt.getDsId());
                        ver.setSchema(dt.getSchema());
                        ver.setTable(dt.getTableName());
                        ver.setMeta(MetaFetcherManager.getMysqlMetaFetcher().fetch(dt.getSchema(), dt.getTableName(), -999));
                        DBFacadeManager.getDbFacade().createMetaVersion(ver);
                    }
                }
            }
        }
    }
    
    private void reloadVersions(List<DataTable> list) throws Exception {
        for (DataTable t : list) {
            String schema = t.getSchema();
            String tableName = t.getTableName();
            Long currentVer = t.getVerId();

            if (currentVer.equals(0L)) {
                try {
                    // 获取该表的版本信息
                    MetaVersion v = DBFacadeManager.getDbFacade().queryMetaVersion(t.getId(), Long.MAX_VALUE, Long.MAX_VALUE);
                    if (v == null) {
                        // 初始化加载
                        int initVersion = -999;
                        MetaWrapper wrapper = MetaFetcherManager.getOraMetaFetcher().fetch(schema, tableName, initVersion);
                        v = new MetaVersion();
                        v.setSchema(schema);
                        v.setTable(tableName);
                        v.setTableId(t.getId());
                        v.setMeta(wrapper);
                        // 将信息保存到数据库
                        DBFacadeManager.getDbFacade().initializeDataTable(t.getId(), v);
                    } else {
                        DBFacadeManager.getDbFacade().updateTableVer(t.getId(), v.getId());
                    }

                } catch (SQLException | MetaFetcherException e) {
                    logger.error("Fetch table[{}.{}] meta information error [{}]", schema, tableName, e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("Fetch table[{}.{}] meta information error [{}]", schema, tableName, e.getMessage(), e);
                    throw e;
                }
            }
        }
    }

    private void filter(List<TabSchema> schemaList) {
        Set<String> availableSchemas = Sets.newHashSet();

        // 获取配置指定的schema,如果没有配置则取数据库中所有schema
        String schemas = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.AVAILABLE_SCHEMAS);
        if (schemas != null) {
            String[] schemaArr = schemas.split(",");
            for (String s : schemaArr) {
                availableSchemas.add(s.trim());
            }
        }
        if (availableSchemas.isEmpty()) return;

        Iterator<TabSchema> it = schemaList.iterator();
        while (it.hasNext()) {
            TabSchema schema = it.next();
            if (!availableSchemas.contains(schema.getSchema())) {
                it.remove();
            }
        }
    }
}
