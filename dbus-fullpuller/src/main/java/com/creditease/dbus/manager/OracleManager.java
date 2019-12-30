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
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.SupportedOraDataType;
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
 * Manages connections to Oracle databases.
 * Requires the Oracle JDBC driver.
 */
public class OracleManager extends GenericSqlManager {

    private static Logger logger = LoggerFactory.getLogger(OracleManager.class);

    /**
     * ORA-00942: Table or view does not exist. Indicates that the user does
     * not have permissions.
     */
    public static final int ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST = 942;

    /**
     * This is a catalog view query to list the databases. For Oracle we map the
     * concept of a database to a schema, and a schema is identified by a user.
     * In order for the catalog view DBA_USERS be visible to the user who executes
     * this query, they must have the DBA privilege.
     */
    public static final String QUERY_LIST_DATABASES = "SELECT USERNAME FROM DBA_USERS";

    /**
     * Query to list all tables visible to the current user. Note that this list
     * does not identify the table owners which is required in order to
     * ensure that the table can be operated on for import/export purposes.
     */
    public static final String QUERY_LIST_TABLES = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?";

    /**
     * Query to list all columns of the given table. Even if the user has the
     * privileges to access table objects from another schema, this query will
     * limit it to explore tables only from within the active schema.
     */
    public static final String QUERY_COLUMNS_FOR_TABLE =
            "SELECT COLUMN_NAME columnName, DATA_TYPE columnTypeName FROM ALL_TAB_COLUMNS WHERE "
                    + "OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID";

    /**
     * Query to find the primary key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
            "SELECT ALL_CONS_COLUMNS.COLUMN_NAME FROM ALL_CONS_COLUMNS, "
                    + "ALL_CONSTRAINTS WHERE ALL_CONS_COLUMNS.CONSTRAINT_NAME = "
                    + "ALL_CONSTRAINTS.CONSTRAINT_NAME AND "
                    + "ALL_CONSTRAINTS.CONSTRAINT_TYPE = 'P' AND "
                    + "ALL_CONS_COLUMNS.TABLE_NAME = ? AND "
                    + "ALL_CONS_COLUMNS.OWNER = ?";

    /**
     * Query to find the UNIQUE key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
            "SELECT c.COLUMN_NAME FROM ALL_IND_COLUMNS c, ALL_INDEXES i, ALL_TAB_COLUMNS t WHERE "
                    + "c.TABLE_NAME = ? and c.TABLE_OWNER = ? "
                    + "AND c.INDEX_NAME = i.INDEX_NAME AND c.TABLE_OWNER = i.TABLE_OWNER AND c.TABLE_NAME = i.TABLE_NAME "
                    + "AND i.UNIQUENESS = 'UNIQUE'"
                    + "AND c.TABLE_OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME and c.COLUMN_NAME = t.COLUMN_NAME";

    /**
     * Query to find the INDEXED column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_INDEXED_COL_FOR_TABLE =
            "SELECT c.COLUMN_NAME FROM ALL_IND_COLUMNS c, ALL_INDEXES i, ALL_TAB_COLUMNS t WHERE "
                    + "c.TABLE_NAME = ? and c.TABLE_OWNER = ? "
                    + "AND c.INDEX_NAME = i.INDEX_NAME AND c.TABLE_OWNER = i.TABLE_OWNER AND c.TABLE_NAME = i.TABLE_NAME "
                    + "AND i.UNIQUENESS = 'NONUNIQUE'"
                    + "AND c.TABLE_OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME and c.COLUMN_NAME = t.COLUMN_NAME";
    /**
     * Query to get the current user for the DB session.   Used in case of
     * wallet logins.
     */
    public static final String QUERY_GET_SESSIONUSER = "SELECT USER FROM DUAL";

    /**
     * driver class to ensure is loaded when making db connection.
     */
    public static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";

    /**
     * Configuration key to use to set the session timezone.
     */
    public static final String ORACLE_TIMEZONE_KEY = "oracle.sessionTimeZone";


    public OracleManager(final DBConfiguration opts, String connectString) {
        super(DRIVER_CLASS, opts, connectString);
        Object scn = opts.get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
        if (scn == null || (Long) scn == 0) {
            logger.warn("Not set SCN yet!");
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    public String[] getColumnNames(String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<String>();

        String tableOwner = null;
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            tableOwner = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }

        try {
            conn = getConnection();
            ps = conn.prepareStatement(QUERY_COLUMNS_FOR_TABLE, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setString(1, tableOwner);
            ps.setString(2, shortTableName);

            rs = ps.executeQuery();
            while (rs.next()) {
                String columnTypeName = rs.getString("columnTypeName");
                if (columnTypeName != null && SupportedOraDataType.isSupported(columnTypeName)) {
                    columns.add(rs.getString("columnName"));
                }
            }
        } catch (SQLException e) {
            LoggingUtils.logAll(logger, "Failed to list columns", e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            close(ps, rs);
        }

        return columns.toArray(new String[columns.size()]);
    }


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

    public MetaWrapper queryMetaInOriginalDb() {
        MetaWrapper metaInOriginalDb = new MetaWrapper();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String schema = dbConfig.getString(DBConfiguration.INPUT_SCHEMA_PROPERTY);
        String table = dbConfig.getString(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY);
        // 对于系列表，任取其中一个表来获取Meta信息。此处取第一个表。
        if (table.indexOf(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            table = table.split(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }
        if (table.indexOf(".") != -1) {
            table = table.split("\\.")[1];
        }
        // 如果meta变了，scn即刻失效，抛异常,会返回false，符合预期。所以这里查meta不用带scn号，来尝试取“某个时刻”的meta。
        String sql = "select t1.owner, " +
                " t1.table_name, " +
                " t1.column_name, " +
                " t1.column_id, " +
                " t1.data_type, " +
                " t1.data_length, " +
                " t1.data_precision, " +
                " t1.data_scale, " +
                " t1.nullable, " +
                " systimestamp, " +
                " decode(t2.is_pk, 1, 'Y', 'N') is_pk, " +
                " decode(t2.position, null, -1, t2.position) pk_position " +
                " from (select t.* " +
                " from dba_tab_columns t " +
                " where t.owner = '" + schema.toUpperCase() + "' " +
                " and t.table_name = '" + table.toUpperCase() + "') t1 " +
                " left join (select cu.table_name,cu.column_name, cu.position, 1 as is_pk " +
                " from dba_cons_columns cu, dba_constraints au " +
                " where cu.constraint_name = au.constraint_name and cu.owner = au.owner  " +
                " and au.constraint_type = 'P' " +
                " and au.table_name = '" + table.toUpperCase() + "' " +
                " and au.owner = '" + schema.toUpperCase() + "') t2 on (t1.column_name = t2.column_name and t1.table_name = t2.table_name) ";

        logger.info("[Oracle manager] Meta query sql is {}.", sql);

        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            while (rs.next()) {
                MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();
                cell.setOwner(rs.getString("owner"));
                cell.setTableName(rs.getString("table_name"));
                cell.setColumnName(rs.getString("column_name"));
                cell.setColumnId(rs.getInt("column_id"));
                cell.setDataType(rs.getString("data_type"));
                cell.setDataLength(rs.getLong("data_length"));
                cell.setDataPrecision(rs.getInt("data_precision"));
                Object scale = rs.getObject("data_scale");
                if (cell.getDataType().equals(SupportedOraDataType.NUMBER.toString()) && scale == null) {
                    cell.setDataScale(-127);
                } else {
                    cell.setDataScale(rs.getInt("data_scale"));
                }

                cell.setNullAble(rs.getString("nullable"));
                cell.setIsPk(rs.getString("is_pk"));
                cell.setPkPosition(rs.getInt("pk_position"));
                metaInOriginalDb.addMetaCell(cell);
            }
        } catch (SQLException e) {
            LoggingUtils.logAll(logger, "Failed to query meta from original DB.", e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            close(ps, rs);
        }
        return metaInOriginalDb;
    }
}
