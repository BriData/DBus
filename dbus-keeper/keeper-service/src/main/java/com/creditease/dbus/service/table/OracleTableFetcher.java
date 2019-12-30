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


package com.creditease.dbus.service.table;

import com.creditease.dbus.domain.model.DataSource;

import java.sql.PreparedStatement;
import java.util.Map;

public class OracleTableFetcher extends TableFetcher {
    public OracleTableFetcher(DataSource ds) {
        super(ds);
    }

    @Override
    public String buildQuery(Object... args) {
        String sql = " select TABLE_NAME from all_tables where owner = ?";
        return sql;
    }

    @Override
    public String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
        statement.setString(1, get(params, "schemaName"));
        return null;
    }

    private String get(Map<String, Object> map, String key) {
        return map.get(key).toString();
    }

    /*
    查询table字段的sql语句
 */
    @Override
    public String buildTableFieldQuery(Object... args) {
        String sql = "select TABLE_NAME,COLUMN_NAME,DATA_TYPE from all_tab_columns where owner= ?";
        return sql;
    }

    /**
     * 填充sql查询字段,schemaName和tableName
     */
    @Override
    public String fillTableParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
        statement.setString(1, get(params, "schemaName"));
        return null;
    }

    @Override
    protected String buildDataRowsQuery(String schemaName, String tableName) {
        return "select num_rows num from all_tables t where owner = '" + schemaName + "' and table_name = '" + tableName + "'";
    }

}
