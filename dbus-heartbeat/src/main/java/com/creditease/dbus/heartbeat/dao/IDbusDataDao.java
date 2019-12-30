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


package com.creditease.dbus.heartbeat.dao;

import com.creditease.dbus.heartbeat.type.MaasMessage.DataSource;
import com.creditease.dbus.heartbeat.type.MaasMessage.SchemaInfo.Column;

import java.util.List;

/**
 * Created by dashencui on 2017/9/14.
 */
public interface IDbusDataDao {
    public boolean testQuery(String key);

    public List<Long> queryDsFromDbusData(String key, String host, String port, String instance_name, String schmaName, String tableName);

    public int queryCountDsId(String key, String host, String port, String instance_name);

    public List<Long> queryDsFromDbusData_use_dsname(String key, String ds_name, String schema_name, String table_name);

    public String queryDsType(String key, String dsName);

    public String queryTableComment(String key, long dsId, String schemaName);

    public String queryTableComment2(String key, long ds_id, String schemaName, String tableName);

    public String queryTableName(String key, long dsId, String schemaName);

    public List<Column> queryColumsUseVerid(String key, long ver_id);

    public List<Column> queryColumsUseMore(String key, long ds_id, String schemaName, String tableName);

    public Column queryColumForAppender(String key, long ds_id, String schemaName, String tableName, String columnName);

    public DataSource queryDsInfoFromDbusData(String key, long ver_id);

    public DataSource queryDsInfoUseDsId(String key, long ds_id);
}
