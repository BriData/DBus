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

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.splitters.DBSplitter;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.InputSplit;
import com.creditease.dbus.common.utils.LoggingUtils;
import com.creditease.dbus.common.utils.ResultSetPrinter;
import com.creditease.dbus.common.utils.SqlTypeMap;
import com.creditease.dbus.common.utils.ValidationException;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.enums.DbusDatasourceType;

/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager
        extends ConnManager {

    private Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * Substring that must appear in free-form queries submitted by users.
     * This is the string '$CONDITIONS'.
     */
    public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

    //protected static final int DEFAULT_FETCH_SIZE = 1000;

    protected DBConfiguration options;
    private Statement lastStatement;
    protected String conString;
    protected static Map<String, DataSource> dataSourceMap = new HashMap<>();

    /**
     * Constructs the SqlManager.
     *
     * @param opts the DBConfiguration describing the user's requested action.
     */
    public SqlManager(final DBConfiguration opts, final String conString) {
        this.options = opts;
        this.conString = conString;
        initOptionDefaults();
    }

    /**
     * Sets default values for values that were not provided by the user.
     * Only options with database-specific defaults should be configured here.
     */
    protected void initOptionDefaults() {
        if (options.get(DBConfiguration.SPLIT_SHARD_SIZE) == null) {
            LOG.info("Using default split shard size: " + DataPullConstants.DEFAULT_SPLIT_SHARD_SIZE);
            options.set(DBConfiguration.SPLIT_SHARD_SIZE, DataPullConstants.DEFAULT_SPLIT_SHARD_SIZE);
        }
    }

    /**
     * @return the SQL query to use in getColumnNames() in case this logic must
     * be tuned per-database, but the main extraction loop is still inheritable.
     */
    protected String getColNamesQuery(String tableName) {
        // adding where clause to prevent loading a big table
        return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t WHERE 1=0";
    }

    @Override
    /** {@inheritDoc} */
    public String[] getColumnNames(String tableName) {
        String stmt = getColNamesQuery(tableName);
        return getColumnNamesForRawQuery(stmt);
    }

    @Override
    /** {@inheritDoc} */
    public String[] getColumnNamesForQuery(String query) {
        String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
        return getColumnNamesForRawQuery(rawQuery);
    }

    /**
     * Get column names for a query statement that we do not modify further.
     */
    public String[] getColumnNamesForRawQuery(String stmt) {
        ResultSet results;
        try {
            results = execute(stmt);
        } catch (SQLException sqlE) {
            LoggingUtils.logAll(LOG, "Error executing statement: " + sqlE.toString(),
                    sqlE);
            release();
            return null;
        } catch (Exception e) {
            release();
            LOG.error(e.getMessage(), e);
            return null;
        }

        try {
            int cols = results.getMetaData().getColumnCount();
            ArrayList<String> columns = new ArrayList<String>();
            ResultSetMetaData metadata = results.getMetaData();
            for (int i = 1; i < cols + 1; i++) {
                String colName = metadata.getColumnLabel(i);
                if (colName == null || colName.equals("")) {
                    colName = metadata.getColumnName(i);
                    if (null == colName) {
                        colName = "_RESULT_" + i;
                    }
                }
                columns.add(colName);
                LOG.debug("Found column " + colName);
            }
            return columns.toArray(new String[0]);
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading from database: "
                    + sqlException.toString(), sqlException);
            return null;
        } finally {
            try {
                results.close();
                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException closing ResultSet: "
                        + sqlE.toString(), sqlE);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.error(e.getMessage(), e);
            }

            release();
        }
    }

    @Override
    public String[] getColumnNamesForProcedure(String procedureName) {
        List<String> ret = new ArrayList<String>();
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null,
                    procedureName, null);
            if (null == results) {
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn) {
                        int index = results.getInt("ORDINAL_POSITION") - 1;
                        if (index < 0) {
                            continue; // actually the return type
                        }
                        for (int i = ret.size(); i < index; ++i) {
                            ret.add(null);
                        }
                        String name = results.getString("COLUMN_NAME");
                        if (index == ret.size()) {
                            ret.add(name);
                        } else {
                            ret.set(index, name);
                        }
                    }
                }
                LOG.debug("getColumnsNamesForProcedure returns "
                        + StringUtils.join(ret, ","));
                return ret.toArray(new String[ret.size()]);
            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException e) {
            LoggingUtils.logAll(LOG, "Error reading procedure metadata: ", e);
            throw new RuntimeException("Can't fetch column names for procedure.", e);
        } catch (Exception e) {
            throw new RuntimeException("Can't fetch column names for procedure.", e);
        }
    }

    /**
     * @return the SQL query to use in getColumnTypes() in case this logic must
     * be tuned per-database, but the main extraction loop is still inheritable.
     */
    protected String getColTypesQuery(String tableName) {
        return getColNamesQuery(tableName);
    }

    @Override
    public Map<String, Integer> getColumnTypes(String tableName) {
        String stmt = getColTypesQuery(tableName);
        return getColumnTypesForRawQuery(stmt);
    }

    @Override
    public Map<String, Integer> getColumnTypesForQuery(String query) {
        // Manipulate the query to return immediately, with zero rows.
        String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
        return getColumnTypesForRawQuery(rawQuery);
    }

    /**
     * Get column types for a query statement that we do not modify further.
     */
    protected Map<String, Integer> getColumnTypesForRawQuery(String stmt) {
        Map<String, List<Integer>> colInfo = getColumnInfoForRawQuery(stmt);
        if (colInfo == null) {
            return null;
        }
        Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
        for (String s : colInfo.keySet()) {
            List<Integer> info = colInfo.get(s);
            colTypes.put(s, info.get(0));
        }
        return colTypes;
    }

    @Override
    public Map<String, List<Integer>> getColumnInfo(String tableName) {
        String stmt = getColNamesQuery(tableName);
        return getColumnInfoForRawQuery(stmt);
    }

    @Override
    public Map<String, List<Integer>> getColumnInfoForQuery(String query) {
        // Manipulate the query to return immediately, with zero rows.
        String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
        return getColumnInfoForRawQuery(rawQuery);
    }

    protected Map<String, List<Integer>> getColumnInfoForRawQuery(String stmt) {
        ResultSet results;
        LOG.debug("Execute getColumnInfoRawQuery : " + stmt);
        try {
            results = execute(stmt);
        } catch (SQLException sqlE) {
            LoggingUtils.logAll(LOG, "Error executing statement: " + sqlE.toString(),
                    sqlE);
            release();
            return null;
        } catch (Exception e) {
            release();
            LOG.error(e.getMessage(), e);
            return null;
        }

        try {
            Map<String, List<Integer>> colInfo = new SqlTypeMap<String, List<Integer>>();

            int cols = results.getMetaData().getColumnCount();
            ResultSetMetaData metadata = results.getMetaData();
            for (int i = 1; i < cols + 1; i++) {
                int typeId = metadata.getColumnType(i);
                int precision = metadata.getPrecision(i);
                int scale = metadata.getScale(i);

                // If we have an unsigned int we need to make extra room by
                // plopping it into a bigint
                if (typeId == Types.INTEGER && !metadata.isSigned(i)) {
                    typeId = Types.BIGINT;
                }

                String colName = metadata.getColumnLabel(i);
                if (colName == null || colName.equals("")) {
                    colName = metadata.getColumnName(i);
                }
                List<Integer> info = new ArrayList<Integer>(3);
                info.add(Integer.valueOf(typeId));
                info.add(precision);
                info.add(scale);
                colInfo.put(colName, info);
                LOG.debug("Found column " + colName + " of type " + info);
            }

            return colInfo;
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading from database: "
                    + sqlException.toString(), sqlException);
            return null;
        } finally {
            try {
                results.close();
                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException closing ResultSet: " + sqlE.toString(), sqlE);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            release();
        }
    }

    @Override
    public Map<String, String> getColumnTypeNamesForTable(String tableName) {
        String stmt = getColTypesQuery(tableName);
        return getColumnTypeNamesForRawQuery(stmt);
    }

    @Override
    public Map<String, String> getColumnTypeNamesForQuery(String query) {
        // Manipulate the query to return immediately, with zero rows.
        String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
        return getColumnTypeNamesForRawQuery(rawQuery);
    }

    protected Map<String, String> getColumnTypeNamesForRawQuery(String stmt) {
        ResultSet results;
        try {
            results = execute(stmt);
        } catch (SQLException sqlE) {
            LoggingUtils.logAll(LOG, "Error executing statement: " + sqlE.toString(),
                    sqlE);
            release();
            return null;
        } catch (Exception e) {
            release();
            LOG.error(e.getMessage(), e);
            return null;
        }

        try {
            Map<String, String> colTypeNames = new HashMap<String, String>();

            int cols = results.getMetaData().getColumnCount();
            ResultSetMetaData metadata = results.getMetaData();
            for (int i = 1; i < cols + 1; i++) {
                String colTypeName = metadata.getColumnTypeName(i);

                String colName = metadata.getColumnLabel(i);
                if (colName == null || colName.equals("")) {
                    colName = metadata.getColumnName(i);
                }

                colTypeNames.put(colName, colTypeName);
                LOG.debug("Found column " + colName + " of type " + colTypeName);
            }

            return colTypeNames;
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading from database: "
                    + sqlException.toString(), sqlException);
            return null;
        } finally {
            try {
                results.close();
                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException closing ResultSet: "
                        + sqlE.toString(), sqlE);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            release();
        }
    }

    @Override
    public ResultSet readTable(String tableName, String[] columns) throws SQLException, Exception {
        if (columns == null) {
            columns = getColumnNames(tableName);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        boolean first = true;
        for (String col : columns) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(escapeColName(col));
            first = false;
        }
        sb.append(" FROM ");
        sb.append(escapeTableName(tableName));
        sb.append(" AS ");   // needed for hsqldb; doesn't hurt anyone else.
        sb.append(escapeTableName(tableName));

        String sqlCmd = sb.toString();
        LOG.debug("Reading table with command: " + sqlCmd);
        return execute(sqlCmd);
    }

    @Override
    public String[] listDatabases() {
        // TODO(aaron): Implement this!
        LOG.error("Generic SqlManager.listDatabases() not implemented.");
        return null;
    }

    @Override
    public Map<String, Integer> getColumnTypesForProcedure(String procedureName) {
        Map<String, List<Integer>> colInfo =
                getColumnInfoForProcedure(procedureName);
        if (colInfo == null) {
            return null;
        }
        Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
        for (String s : colInfo.keySet()) {
            List<Integer> info = colInfo.get(s);
            colTypes.put(s, info.get(0));
        }
        return colTypes;
    }

    @Override
    public Map<String, List<Integer>> getColumnInfoForProcedure(String procedureName) {
        Map<String, List<Integer>> ret = new TreeMap<String, List<Integer>>();
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null, procedureName, null);
            if (null == results) {
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn
                            && results.getInt("ORDINAL_POSITION") > 0) {
                        // we don't care if we get several rows for the
                        // same ORDINAL_POSITION (e.g. like H2 gives us)
                        // as we'll just overwrite the entry in the map:
                        List<Integer> info = new ArrayList<Integer>(3);
                        info.add(results.getInt("DATA_TYPE"));
                        info.add(results.getInt("PRECISION"));
                        info.add(results.getInt("SCALE"));
                        ret.put(results.getString("COLUMN_NAME"), info);
                    }
                }
                LOG.debug("Columns returned = " + StringUtils.join(ret.keySet(), ","));
                LOG.debug("Types returned = " + StringUtils.join(ret.values(), ","));
                return ret.isEmpty() ? null : ret;
            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Map<String, String> getColumnTypeNamesForProcedure(String procedureName) {
        Map<String, String> ret = new TreeMap<String, String>();
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null,
                    procedureName, null);
            if (null == results) {
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn
                            && results.getInt("ORDINAL_POSITION") > 0) {
                        // we don't care if we get several rows for the
                        // same ORDINAL_POSITION (e.g. like H2 gives us)
                        // as we'll just overwrite the entry in the map:
                        ret.put(
                                results.getString("COLUMN_NAME"),
                                results.getString("TYPE_NAME"));
                    }
                }
                LOG.debug("Columns returned = " + StringUtils.join(ret.keySet(), ","));
                LOG.debug(
                        "Type names returned = " + StringUtils.join(ret.values(), ","));
                return ret.isEmpty() ? null : ret;
            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String[] listTables() {
        ResultSet results = null;
        String[] tableTypes = {"TABLE"};
        try {
            try {
                DatabaseMetaData metaData = this.getConnection().getMetaData();
                results = metaData.getTables(null, null, null, tableTypes);
            } catch (SQLException sqlException) {
                LoggingUtils.logAll(LOG, "Error reading database metadata: "
                        + sqlException.toString(), sqlException);
                return null;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            if (null == results) {
                return null;
            }

            try {
                ArrayList<String> tables = new ArrayList<String>();
                while (results.next()) {
                    String tableName = results.getString("TABLE_NAME");
                    tables.add(tableName);
                }

                return tables.toArray(new String[0]);
            } catch (SQLException sqlException) {
                LoggingUtils.logAll(LOG, "Error reading from database: "
                        + sqlException.toString(), sqlException);
                return null;
            }
        } finally {
            if (null != results) {
                try {
                    results.close();
                    getConnection().commit();
                } catch (SQLException sqlE) {
                    LoggingUtils.logAll(LOG, "Exception closing ResultSet: "
                            + sqlE.toString(), sqlE);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public String getPrimaryKey(String tableName) {
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getPrimaryKeys(null, null, tableName);
            if (null == results) {
                return null;
            }

            try {
                if (results.next()) {
                    return results.getString("COLUMN_NAME");
                } else {
                    return null;
                }
            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Retrieve the actual connection from the outer ConnManager.
     */
    public abstract Connection getConnection() throws SQLException, Exception;

    public abstract String getIndexedColQuery(String indexType);

    public abstract List<String> queryTablePartitions(String sql);

    public String queryIndexedColForSplit(String tableName, String indexType) {
        String splitCol = null;
        Connection conn = null;
        PreparedStatement pStmt = null;
        PreparedStatement pStmtOracle = null;
        ResultSet rset = null;
        ResultSet rsetOracle = null;
        List<String> columns = new ArrayList<String>();

        String schema = null;
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            schema = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }

        try {
            conn = getConnection();

            // if (tableOwner == null) {
            // tableOwner = getSessionUser(conn);
            // }
            String indexedColQuery = getIndexedColQuery(indexType);

            LOG.info("schema is:{}, shortTableName is:{}, indexedColQuery is:{}.", schema, shortTableName,
                    indexedColQuery);

            pStmt = conn.prepareStatement(indexedColQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, shortTableName);
            pStmt.setString(2, schema);
            rset = pStmt.executeQuery();

            while (rset.next()) {
                columns.add(rset.getString(1));
            }
            conn.commit();


            if (columns.size() == 0) {
                // Table has no primary key
                LOG.warn("Table has no  key, type:" + indexType);
                return null;
            } else if (columns.size() == 1) {
                LOG.info("find split column :" + columns.get(0) + ", Index type:" + indexType);
            } else {
                //columns.size() > 1

                // The primary key is multi-column primary key. Warn the user.
                // TODO select the appropriate column instead of the first column
                // based
                // on the datatype - giving preference to numerics over other types.
                LOG.warn("The table " + tableName + " " + "contains a multi-column key. Will default to "
                        + "the column " + columns.get(0) + " only for this job." + ", type:" + indexType);
            }


            splitCol = columns.get(0);

            String datasourceType = options.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            if (dataBaseType == DbusDatasourceType.ORACLE) {
                // 对于ORACLE数据库，目前只有整数类型比较友好。区别对待下整数类型分片列和其他类型分片列
                String splitColTypeDetectQuery = "select " + splitCol + " from " + tableName + " where rownum <= 1";
                pStmtOracle = conn.prepareStatement(splitColTypeDetectQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                rsetOracle = pStmtOracle.executeQuery();

                while (rsetOracle.next()) {
                    int splitColSqlDataType = rsetOracle.getMetaData().getColumnType(1);
                    if (splitColSqlDataType == Types.INTEGER
                            || splitColSqlDataType == Types.TINYINT
                            || splitColSqlDataType == Types.SMALLINT
                            || splitColSqlDataType == Types.BIGINT
                            || splitColSqlDataType == Types.NUMERIC
                            || splitColSqlDataType == Types.DECIMAL
                            || splitColSqlDataType == Types.REAL
                            || splitColSqlDataType == Types.FLOAT
                            || splitColSqlDataType == Types.DOUBLE) {
                        // 对于上述数字类型，DBUS根据 splitCol 按分片策略分片并发拉取。
                        // 此处故意留白
                        LOG.info("Found split column data type is {}(Numeric):", splitColSqlDataType);
                    } else {
                        // 对于整数以外的其它类型，将splitCol设为null。后续逻辑认为没有合适的分片列。将不对数据进行分片，所有数据作一片拉取。
                        splitCol = null;
                        LOG.info("Found split column data type is {}(None Numeric):", splitColSqlDataType);
                    }
                }
                conn.commit();
            }
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to query indexed col", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(), e);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
                }
            }
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }

            if (rsetOracle != null) {
                try {
                    rsetOracle.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
                }
            }
            if (pStmtOracle != null) {
                try {
                    pStmtOracle.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }

            // 关闭connecttion的事由druid去做
            // try {
            // close();
            // } catch (SQLException ex) {
            // LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
            // }
            // catch (Exception e) {
            // // TODO Auto-generated catch block
            // LOG.error(e.getMessage(),e);
            // }
        }
        return splitCol;
    }

    /**
     * Determine what column to use to split the table.
     *
     * @return the splitting column, if one is set or inferrable, or null
     * otherwise.
     */
    public String getSplitColumn() {
        String splitCol = options.getString(DBConfiguration.INPUT_SPLIT_COL);
        if (StringUtils.isNotBlank(splitCol)) {
            return splitCol;
        }

        String tableName = options.getString(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY);
        // 对于系列表，任取其中一个表来获取Meta信息。此处取第一个表。
        if (tableName.indexOf(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            tableName = tableName.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }

        if (tableName != null) {
            splitCol = queryIndexedColForSplit(tableName, DataPullConstants.SPLIT_COL_TYPE_PK);
            if (null == splitCol) {
                splitCol = queryIndexedColForSplit(tableName.toUpperCase(), DataPullConstants.SPLIT_COL_TYPE_PK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColForSplit(tableName, DataPullConstants.SPLIT_COL_TYPE_UK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColForSplit(tableName.toUpperCase(), DataPullConstants.SPLIT_COL_TYPE_UK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColForSplit(tableName, DataPullConstants.SPLIT_COL_TYPE_COMMON_INDEX);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColForSplit(tableName.toUpperCase(), DataPullConstants.SPLIT_COL_TYPE_COMMON_INDEX);
            }
        }

        if (StringUtils.isBlank(splitCol)) {
            splitCol = "";
        }
        options.set(DBConfiguration.INPUT_SPLIT_COL, splitCol);
        LOG.info("getSplitColumn() set split col is : {}", splitCol);
        return splitCol;
    }

    /**
     * Offers the ConnManager an opportunity to validate that the
     * options specified in the ImportJobContext are valid.
     * @throws ImportException if the import is misconfigured.
     */
//  public void checkTableImportOptions()
//      throws IOException, ImportException {
//    // Default implementation: check that the split column is set
//    // correctly.
//    String splitCol = getSplitColumn();
//    String tableName = options.getString(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
//    int numMappers = options.getInputSplitMappersNum(); 
//    boolean autoSetMappersNumToOne=options.getBoolean(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS_AUTO_SET_TO_ONE,false); 
//    
//    if (null == splitCol && numMappers > 1) {
//      if (!autoSetMappersNumToOne) {
//        // Can't infer a primary key.
//        throw new ImportException("No primary key could be found for table "
//          + tableName + ". Please specify one with --split-by or perform "
//          + "a sequential import with '-m 1'.");
//      } else {
//        LOG.warn("Split by column not provided or can't be inferred.  Resetting to one mapper");
//        options.set(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS, "1");
//      }
//    }
//  }

    /**
     * Executes an arbitrary SQL statement.
     *
     * @param stmt      The SQL statement to execute
     * @param fetchSize Overrides default or parameterized fetch size
     * @return A ResultSet encapsulating the results or null on error
     */
    protected ResultSet execute(String stmt, Integer fetchSize, Object... args)
            throws SQLException, Exception {
        // Release any previously-open statement.
        release();

        PreparedStatement statement = null;
        statement = this.getConnection().prepareStatement(stmt,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize != null) {
            LOG.debug("Using fetchSize for next query: " + fetchSize);
            statement.setFetchSize(fetchSize);
        }
        this.lastStatement = statement;
        if (null != args) {
            for (int i = 0; i < args.length; i++) {
                statement.setObject(i + 1, args[i]);
            }
        }

        LOG.info("Executing SQL statement: " + stmt);
        return statement.executeQuery();
    }

    /**
     * 使用connection 返回一个 statment用于后续使用，改statement 由lastStatement 管理
     *
     * @param stmt
     * @return
     * @throws Exception
     */
    public PreparedStatement prepareStatement(String stmt) throws Exception {
        release();

        PreparedStatement statement = this.getConnection().prepareStatement(stmt,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.lastStatement = statement;

        return statement;
    }

    /**
     * Executes an arbitrary SQL Statement.
     *
     * @param stmt The SQL statement to execute
     * @return A ResultSet encapsulating the results or null on error
     */
    protected ResultSet execute(String stmt, Object... args) throws SQLException, Exception {
        return execute(stmt, options.getInt(DBConfiguration.SPLIT_SHARD_SIZE, DataPullConstants.DEFAULT_SPLIT_SHARD_SIZE), args);
    }

    public void close() throws SQLException {
        release();
    }

    /**
     * Prints the contents of a ResultSet to the specified PrintWriter.
     * The ResultSet is closed at the end of this method.
     *
     * @param results the ResultSet to print.
     * @param pw      the location to print the data to.
     */
    protected void formatAndPrintResultSet(ResultSet results, PrintWriter pw) {
        try {
            try {
                int cols = results.getMetaData().getColumnCount();
                pw.println("Got " + cols + " columns back");
                if (cols > 0) {
                    ResultSetMetaData rsmd = results.getMetaData();
                    String schema = rsmd.getSchemaName(1);
                    String table = rsmd.getTableName(1);
                    if (null != schema) {
                        pw.println("Schema: " + schema);
                    }

                    if (null != table) {
                        pw.println("Table: " + table);
                    }
                }
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException reading result metadata: "
                        + sqlE.toString(), sqlE);
            }

            try {
                new ResultSetPrinter().printResultSet(pw, results);
            } catch (IOException ioe) {
                LOG.error("IOException writing results: " + ioe.toString());
                return;
            }
        } finally {
            try {
                results.close();
                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException closing ResultSet: "
                        + sqlE.toString(), sqlE);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.error(e.getMessage(), e);
            }

            release();
        }
    }

    /**
     * Poor man's SQL query interface; used for debugging.
     *
     * @param s the SQL statement to execute.
     */
    public void execAndPrint(String s) {
        ResultSet results = null;
        try {
            results = execute(s);
        } catch (SQLException sqlE) {
            LoggingUtils.logAll(LOG, "Error executing statement: ", sqlE);
            release();
            return;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            release();
            return;
        }

        PrintWriter pw = new PrintWriter(System.out, true);
        try {
            formatAndPrintResultSet(results, pw);
        } finally {
            pw.close();
        }
    }

    /**
     * Create a connection to the database; usually used only from within
     * getConnection(), which enforces a singleton guarantee around the
     * Connection object.
     */
    protected Connection makeConnection() throws SQLException, Exception {

        Connection connection;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }

        String username = options.getString(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY);
        String password = options.getString(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY);
//    String connectString = options.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY);
//    if(!readOnly){
//        connectString = (String)(options.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE));
//    }
        String connectionParamsStr = options.getString(DBConfiguration.CONNECTION_PARAMS_PROPERTY);
        Properties connectionParams = DBConfiguration.propertiesFromString(connectionParamsStr);

        if (connectionParams != null && connectionParams.size() > 0) {
            LOG.debug("User specified connection params. "
                    + "Using properties specific API for making connection.");

            Properties props = new Properties();
            if (username != null) {
                props.put("user", username);
            }

            if (password != null) {
                props.put("password", password);
            }

            props.putAll(connectionParams);
            connection = DriverManager.getConnection(this.conString, props);
        } else {
            LOG.debug("No connection paramenters specified. "
                    + "Using regular API for making connection.");
            if (username == null) {
                connection = DriverManager.getConnection(this.conString);
            } else {
                connection = DriverManager.getConnection(
                        this.conString, username, password);
            }
        }

        // We only use this for metadata queries. Loosest semantics are okay.
        connection.setTransactionIsolation(getMetadataIsolationLevel());
        connection.setAutoCommit(false);

        return connection;
    }

    /**
     * @return the transaction isolation level to use for metadata queries
     * (queries executed by the ConnManager itself).
     */
    protected int getMetadataIsolationLevel() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    public void release() {
        if (null != this.lastStatement) {
            try {
                this.lastStatement.close();
            } catch (SQLException e) {
                LoggingUtils.logAll(LOG, "Exception closing executed Statement: "
                        + e, e);
            }

            this.lastStatement = null;
        }
    }

    /**
     * @return a SQL query to retrieve the current timestamp from the db.
     */
    protected String getCurTimestampQuery() {
        return "SELECT CURRENT_TIMESTAMP()";
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public Timestamp getCurrentDbTimestamp() {
        release(); // Release any previous ResultSet.

        Statement stmt = null;
        ResultSet rs = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(getCurTimestampQuery());
            if (rs == null || !rs.next()) {
                return null; // empty ResultSet.
            }

            return rs.getTimestamp(1);
        } catch (SQLException sqlE) {
            LoggingUtils.logAll(LOG, "SQL exception accessing current timestamp: "
                    + sqlE, sqlE);
            return null;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(), e);
            return null;
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQL Exception closing resultset: "
                        + sqlE, sqlE);
            }

            try {
                if (null != stmt) {
                    stmt.close();
                }
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQL Exception closing statement: "
                        + sqlE, sqlE);
            }
        }
    }

    @Override
    public long getTableRowCount(String tableName) throws SQLException, Exception {
        release(); // Release any previous ResultSet

        // Escape used table name
        tableName = escapeTableName(tableName);

        long result = -1;
        String countQuery = "SELECT COUNT(*) FROM " + tableName;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            rset = stmt.executeQuery(countQuery);
            if (rset.next()) {
                result = rset.getLong(1);
            }
        } catch (SQLException ex) {
            LoggingUtils.logAll(LOG, "Unable to query count * for table "
                    + tableName, ex);
            throw ex;
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Unable to close result set", ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Unable to close statement", ex);
                }
            }
        }
        return result;
    }

    @Override
    public void deleteAllRecords(String tableName) throws SQLException, Exception {
        release(); // Release any previous ResultSet

        // Escape table name
        tableName = escapeTableName(tableName);

        String deleteQuery = "DELETE FROM " + tableName;
        Statement stmt = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            int updateCount = stmt.executeUpdate(deleteQuery);
            conn.commit();
            LOG.info("Deleted " + updateCount + " records from " + tableName);
        } catch (SQLException ex) {
            LoggingUtils.logAll(LOG, "Unable to execute delete query: "
                    + deleteQuery, ex);
            throw ex;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Unable to close statement", ex);
                }
            }
        }
    }

    @Override
    public void migrateData(String fromTable, String toTable) throws SQLException, Exception {
        release(); // Release any previous ResultSet

        // Escape all table names
        fromTable = escapeTableName(fromTable);
        toTable = escapeTableName(toTable);

        String updateQuery = "INSERT INTO " + toTable + " ( SELECT * FROM " + fromTable + " )";
        String deleteQuery = "DELETE FROM " + fromTable;
        Statement stmt = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();

            // Insert data from the fromTable to the toTable
            int updateCount = stmt.executeUpdate(updateQuery);
            LOG.info("Migrated " + updateCount + " records from " + fromTable
                    + " to " + toTable);

            // Delete the records from the fromTable
            int deleteCount = stmt.executeUpdate(deleteQuery);

            // If the counts do not match, fail the transaction
            if (updateCount != deleteCount) {
                conn.rollback();
                throw new RuntimeException("Inconsistent record counts");
            }
            conn.commit();
        } catch (SQLException ex) {
            LoggingUtils.logAll(LOG, "Unable to migrate data from "
                    + fromTable + " to " + toTable, ex);
            throw ex;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Unable to close statement", ex);
                }
            }
        }
    }

//  public String getInputBoundsQuery(String splitByCol, String sanitizedQuery) {
//    return options.getString(DBConfiguration.INPUT_BOUNDING_QUERY);
//  }

    @Deprecated
    public static long getCurrentScn(Connection connection) throws SQLException {
        String sql = "SELECT current_scn FROM v$database";
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();

        if (resultSet.next()) {
            long result = resultSet.getLong(1);
            resultSet.close();
            statement.close();
            return result;
        }
        return 0L;
    }

    public abstract ResultSet executeSql(String sql);

    public void writeBackOriginalDb(String startTime, String completedTime, String pullStatus, String errorMsg) {
        String driverClass = getDriverClass();

        String sqlStr = null;

        if (driverClass.equals(DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.ORACLE))) {
            sqlStr = getOracleWriteBackSql(startTime, completedTime, pullStatus, errorMsg);
        } else if (driverClass.equals(DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.MYSQL))) {
            sqlStr = getMysqlWriteBackSql(startTime, completedTime, pullStatus, errorMsg);
        }
        else {
            assert (false); //not suppose to be here
        }

        if (null == sqlStr) {
            return;
        }

        Connection conn = null;
        PreparedStatement pStmt = null;
        long seqno = options.getLong(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SEQNO, 0L);
        try {
            conn = getConnection();
            pStmt = conn.prepareStatement(sqlStr,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            setFullPullReqTblSqlParam(pStmt, startTime, completedTime, pullStatus, errorMsg, seqno);
            pStmt.execute();
            conn.commit();
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to write back original DB.", e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }

//          try {
//            close();
//          } catch (SQLException ex) {
//            LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//          }
//          catch (Exception e) {
//              // TODO Auto-generated catch block
//              LOG.error(e.getMessage(),e);
//          }
        }
    }

    @Deprecated
    public String getPhysicalTables() {
        String physicalTables = null;
        long seqNo = options.getLong(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SEQNO, 0L);
        String sqlStr = "SELECT PHYSICAL_TABLES FROM DB_FULL_PULL_REQUESTS WHERE SEQNO=" + seqNo;

        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet results = null;
        try {
            conn = getConnection();
            pStmt = conn.prepareStatement(sqlStr, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            results = pStmt.executeQuery();
            while (results.next()) {
                physicalTables = results.getString(1);
            }
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to write back original DB.", e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                results.close();
                pStmt.close();
                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException happend on closing resource: "
                        + sqlE.toString(), sqlE);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return physicalTables;
    }

    public long queryTotalRows(String table, String splitCol, String tablePartition) {
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet results = null;
        long totalCountOfCurShard = 0;
        try {
            String query = getTotalRowsCountQuery(table, splitCol, tablePartition);
            LOG.info("queryTotalRows(), query: {}", query);
            conn = getConnection();
            pStmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            results = pStmt.executeQuery();
            if (results.next()) {
                totalCountOfCurShard = results.getLong("TOTALCOUNT");
                LOG.info("queryTotalRows(), totalRows : {}", totalCountOfCurShard);
            }
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to query total rows.", e);
        } catch (Exception e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            }

            LOG.error(e.getMessage(), e);
        } finally {
            try {
                if (results != null)
                    results.close();
                if (pStmt != null)
                    pStmt.close();

                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException happend on closing resource: " + sqlE.toString(), sqlE);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return totalCountOfCurShard;
    }

    public List<InputSplit> querySplits(String table, String splitCol, String tablePartition, String splitterStyle,
                                        String pullCollate, int numSplitsOfCurShard, DataDrivenDBInputFormat dataDrivenDBInputFormat) throws Exception {
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet results = null;
        List<InputSplit> inputSplitListOfCurShard = new ArrayList();
        try {
            String query = getBoundingValsQuery(table, splitCol, tablePartition);
            LOG.info("BoundingValsQuery: " + query);

            conn = getConnection();
            pStmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            results = pStmt.executeQuery();

            // Based on the type of the results, use a different mechanism
            // for interpolating split points (i.e., numeric splits, text splits,
            // dates, etc.)
            int sqlDataType = results.getMetaData().getColumnType(1);
            if (results.next()) {
                // Based on the type of the results, use a different mechanism
                // for interpolating split points (i.e., numeric splits, text splits,
                // dates, etc.)
                boolean isSigned = results.getMetaData().isSigned(1);
                // MySQL has an unsigned integer which we need to allocate space for
                if (sqlDataType == Types.INTEGER && !isSigned) {
                    sqlDataType = Types.BIGINT;
                }
            }
            DBSplitter splitter = dataDrivenDBInputFormat.getSplitter(sqlDataType, options.getInputSplitLimit(), splitterStyle);
            if (null == splitter) {
                throw new IOException("Does not have the splitter for the given"
                        + " SQL data type. Please use either different split column (argument"
                        + " --split-by) or lower the number of mappers to 1. Unknown SQL data"
                        + " type: " + sqlDataType);
            }
            try {
                inputSplitListOfCurShard = splitter.split(numSplitsOfCurShard, results, splitCol, options);
                for (InputSplit inputSplit : inputSplitListOfCurShard) {
                    inputSplit.setTargetTableName(table);
                    inputSplit.setCollate(pullCollate);
                    inputSplit.setTablePartitionInfo(tablePartition);
                }
                LOG.info("Physical Table:{}.{} , {} shards generated.", table, tablePartition, numSplitsOfCurShard);
            } catch (ValidationException e) {
                throw new IOException(e);
            } catch (Exception e) {
                throw new IOException(e);
            }
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to write back original DB.", e);
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (results != null)
                    results.close();
                if (pStmt != null)
                    pStmt.close();

                getConnection().commit();
            } catch (SQLException sqlE) {
                LoggingUtils.logAll(LOG, "SQLException happend on closing resource: " + sqlE.toString(), sqlE);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return inputSplitListOfCurShard;
    }

    private void setFullPullReqTblSqlParam(PreparedStatement pStmt, String startTime, String completedTime,
                                           String pullStatus, String errorMsg, long seqno) throws SQLException {
        int paraIndex = 1;
        if (pStmt != null) {
            if (StringUtils.isNotEmpty(startTime)) {
                pStmt.setString(paraIndex, startTime);
                paraIndex++;
            }
            if (StringUtils.isNotEmpty(completedTime)) {
                pStmt.setString(paraIndex, completedTime);
                paraIndex++;
            }
            if (StringUtils.isNotEmpty(pullStatus)) {
                pStmt.setString(paraIndex, pullStatus);
                paraIndex++;
            }
            if (StringUtils.isNotEmpty(errorMsg)) {
                errorMsg = errorMsg.length() > 1000 ? errorMsg.substring(0, 1000) : errorMsg;
                pStmt.setString(paraIndex, errorMsg);
                paraIndex++;
            }
            pStmt.setLong(paraIndex, seqno);
        }
    }


    private String getOracleWriteBackSql(String startTime, String completedTime, String pullStatus, String errorMsg) {
        if (StringUtils.isEmpty(startTime) && StringUtils.isEmpty(completedTime) &&
                StringUtils.isEmpty(pullStatus) && StringUtils.isEmpty(errorMsg)) {
            return null;
        }
        //      String dbSchema = (String)(options.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
        //      StringBuilder sqlStr = new StringBuilder("UPDATE "+dbSchema+".DB_FULL_PULL_REQUESTS SET ");
        StringBuilder sqlStr = new StringBuilder("UPDATE DB_FULL_PULL_REQUESTS SET ");
        boolean needComma = false;
        if (StringUtils.isNotEmpty(startTime)) {
            needComma = true;
            sqlStr.append("PULL_START_TIME=to_timestamp(?,'yyyy-MM-dd hh24:mi:ss.ff3')");
        }
        if (StringUtils.isNotEmpty(completedTime)) {
            if (needComma) {
                sqlStr.append(",PULL_END_TIME=to_timestamp(?,'yyyy-MM-dd hh24:mi:ss.ff3')");
            } else {
                sqlStr.append("PULL_END_TIME=to_timestamp(?,'yyyy-MM-dd hh24:mi:ss.ff3')");
            }
            needComma = true;
        }
        if (StringUtils.isNotEmpty(pullStatus)) {
            if (needComma) {
                sqlStr.append(",PULL_STATUS=?");
            } else {
                sqlStr.append("PULL_STATUS=?");
            }
            needComma = true;
        }
        if (StringUtils.isNotEmpty(errorMsg)) {
            if (needComma) {
                // sqlStr.append(",PULL_REMARK=PULL_REMARK||'-'||?");
                sqlStr.append(",PULL_REMARK=?");
            } else {
                // sqlStr.append("PULL_REMARK=PULL_REMARK||'-'||?");
                sqlStr.append("PULL_REMARK=?");
            }
            needComma = true;
        }
        sqlStr.append(" WHERE SEQNO=?");
        return sqlStr.toString();
    }

    private String getMysqlWriteBackSql(String startTime, String completedTime, String pullStatus, String errorMsg) {
        if (StringUtils.isEmpty(startTime) && StringUtils.isEmpty(completedTime) &&
                StringUtils.isEmpty(pullStatus) && StringUtils.isEmpty(errorMsg)) {
            return null;
        }

//      String dbSchema = (String)(options.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
//      StringBuilder sqlStr = new StringBuilder("UPDATE "+dbSchema+".db_full_pull_requests SET ");
        StringBuilder sqlStr = new StringBuilder("UPDATE db_full_pull_requests SET ");
        boolean needComma = false;
        if (StringUtils.isNotEmpty(startTime)) {
            needComma = true;
            sqlStr.append("PULL_START_TIME=str_to_date(?,'%Y-%m-%d %H:%i:%s.%f')");
        }
        if (StringUtils.isNotEmpty(completedTime)) {
            if (needComma) {
                sqlStr.append(",PULL_END_TIME=str_to_date(?,'%Y-%m-%d %H:%i:%s.%f')");
            } else {
                sqlStr.append("PULL_END_TIME=str_to_date(?,'%Y-%m-%d %H:%i:%s.%f')");
            }
            needComma = true;
        }
        if (StringUtils.isNotEmpty(pullStatus)) {
            if (needComma) {
                sqlStr.append(",PULL_STATUS=?");
            } else {
                sqlStr.append("PULL_STATUS=?");
            }
            needComma = true;
        }
        if (StringUtils.isNotEmpty(errorMsg)) {
            if (needComma) {
                // sqlStr.append(",PULL_REMARK=concat(PULL_REMARK,'-',?)");
                sqlStr.append(",PULL_REMARK=?");
            } else {
                // sqlStr.append("PULL_REMARK=concat(PULL_REMARK,'-',?)");
                sqlStr.append("PULL_REMARK=?");
            }
            needComma = true;
        }
        sqlStr.append(" WHERE SEQNO=?");
        return sqlStr.toString();
    }

    private String getTotalRowsCountQuery(String table, String splitCol, String tablePartition) {
        StringBuilder query = new StringBuilder();

        //      String splitCol = getDBConf().getInputOrderBy();
        if (StringUtils.isNotBlank(splitCol)) {
            query.append("SELECT COUNT(").append(splitCol).append(") TOTALCOUNT FROM ");
        } else {
            query.append("SELECT COUNT(*) TOTALCOUNT FROM ");
        }
        //      query.append(getDBConf().getInputTableName());
        query.append(table);
        if (StringUtils.isNotBlank(tablePartition)) {
            query.append(" PARTITION (").append(tablePartition).append(") ");
        }

        Object consistentReadScn = options.get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
        if (consistentReadScn != null) {
            query.append(" AS OF SCN ").append((Long) consistentReadScn).append(" ");
        }
        String conditions = options.getInputConditions();
        if (StringUtils.isNotBlank(conditions)) {
            query.append(" WHERE ( " + conditions + " )");
        }
        return query.toString();
    }

    /**
     * @return a query which returns the minimum and maximum values for
     * the order-by column.
     * <p>
     * The min value should be in the first column, and the
     * max value should be in the second column of the results.
     */
    private String getBoundingValsQuery(String table, String splitCol, String tablePartition) {
        // If the user has provided a query, use that instead.
//    String userQuery = getDBConf().getInputBoundingQuery();
//    if (null != userQuery) {
//      return userQuery;
//    }

        // Auto-generate one based on the table name we've been provided with.
        StringBuilder query = new StringBuilder();

//    String splitCol = getDBConf().getInputOrderBy();
        query.append("SELECT MIN(").append(splitCol).append("), ");
        query.append("MAX(").append(splitCol).append(") FROM ");
        query.append(table);
        if (StringUtils.isNotBlank(tablePartition)) {
            query.append(" PARTITION (").append(tablePartition).append(") ");
        }
//    Object consistentReadScn = getDBConf().get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
//    if(consistentReadScn!=null){
//        query.append(" AS OF SCN ").append((Long)consistentReadScn).append(" ");    
//    }

        String conditions = options.getInputConditions();
        if (StringUtils.isNotBlank(conditions)) {
            query.append(" WHERE ( " + conditions + " )");
        }

        return query.toString();
    }
}
