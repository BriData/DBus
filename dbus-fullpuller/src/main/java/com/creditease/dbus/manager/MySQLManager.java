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


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.manager;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.commons.SupportedMysqlDataType;
import com.creditease.dbus.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages connections to MySQL databases.
 */
public class MySQLManager extends GenericSqlManager {

    private static Logger logger = LoggerFactory.getLogger(MySQLManager.class);

    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    public MySQLManager(final DBConfiguration opts, String connectString) {
        super(DRIVER_CLASS, opts, connectString);
    }

    /**
     * Query to find the primary key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
            "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_NAME = ? "
                    + "AND TABLE_SCHEMA = ? "
                    + "AND COLUMN_KEY = 'PRI'";

    /**
     * Query to find the UNIQUE key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
            "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_NAME = ? "
                    + "AND TABLE_SCHEMA = ? "
                    + "AND COLUMN_KEY = 'UNI'";

    /**
     * Query to find the INDEXED column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_INDEXED_COL_FOR_TABLE =
            "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_NAME = ? "
                    + "AND TABLE_SCHEMA = ? "
                    + "AND COLUMN_KEY = 'MUL'";


    @Override
    public String getIndexedColQuery(String indexType) {
        if (FullPullConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (FullPullConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (FullPullConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    /**
     * When using a column name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a column named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param colName the column name as provided by the user, etc.
     * @return how the column name should be rendered in the sql text.
     */
    public String escapeColName(String colName) {
        if (null == colName) {
            return null;
        }
        return "`" + colName + "`";
    }

    /**
     * When using a table name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a table named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param tableName the table name as provided by the user, etc.
     * @return how the table name should be rendered in the sql text.
     */
    public String escapeTableName(String tableName) {
        if (null == tableName) {
            return null;
        }
        return "`" + tableName + "`";
    }

    @Override
    public String[] getColumnNames(String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<String>();
        String schema = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
        String sql = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA = '" + schema + "' "
                + "  AND TABLE_NAME = '" + tableName + "' ";

        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString(1);
                String columnDataType = rs.getString(2);
                if (SupportedMysqlDataType.isSupported(columnDataType)) {
                    columns.add(columnName);
                }
            }
        } catch (SQLException sqle) {
            LoggingUtils.logAll(logger, "Failed to get column name with query: " + sql, sqle);
            throw new RuntimeException(sqle);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            close(ps, rs);
        }

        return columns.toArray(new String[columns.size()]);
    }

}

