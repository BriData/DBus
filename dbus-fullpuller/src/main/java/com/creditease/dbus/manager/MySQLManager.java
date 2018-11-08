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

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.SupportedMysqlDataType;
import com.creditease.dbus.dbaccess.DataSourceProvider;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

/**
 * Manages connections to MySQL databases.
 */
public class MySQLManager
        extends InformationSchemaManager {

    public static final Log LOG = LogFactory.getLog(MySQLManager.class.getName());

    // driver class to ensure is loaded when making db connection.
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";


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

    // set to true after we warn the user that we can use direct fastpath.
    private static boolean warningPrinted = false;

    public MySQLManager(final DBConfiguration opts, String conString) {
        super(DRIVER_CLASS, opts, conString);
    }

    @Override
    public String getIndexedColQuery(String indexType) {
        if (DataPullConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (DataPullConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (DataPullConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    @Override
    protected String getPrimaryKeyQuery(String tableName) {
        return "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA = (" + getSchemaQuery() + ") "
                + "AND TABLE_NAME = '" + tableName + "' "
                + "AND COLUMN_KEY = 'PRI'";
    }

    @Override
    protected String getColNamesQuery(String tableName) {
        // Use mysql-specific hints and LIMIT to return fast
        return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t LIMIT 1";
    }

    /**
     * Create a connection to the database; usually used only from within
     * getConnection(), which enforces a singleton guarantee around the
     * Connection object.
     * <p>
     * Oracle-specific driver uses READ_COMMITTED which is the weakest
     * semantics Oracle supports.
     *
     * @throws Exception
     * @throws SQLException
     */
    protected Connection makeConnection() throws SQLException, Exception {
        Connection connection;
        String driverClass = getDriverClass();
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: " + driverClass);
        }
        String username = (String) (options.get(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY));
        String password = (String) (options.get(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY));

        DataSource ds = dataSourceMap.get(this.conString);
        if (ds == null) {
            Properties props = FullPullHelper.getFullPullProperties(DataPullConstants.ZK_NODE_NAME_MYSQL_CONF, true);
            if (username != null) {
                props.setProperty(Constants.DB_CONF_PROP_KEY_USERNAME, username);
            }
            if (password != null) {
                props.setProperty(Constants.DB_CONF_PROP_KEY_PASSWORD, password);
            }
            props.setProperty(Constants.DB_CONF_PROP_KEY_URL, this.conString);

            DataSourceProvider provider = new DruidDataSourceProvider(props);
            ds = provider.provideDataSource();
            dataSourceMap.put(this.conString, ds);
        }
        connection = ds.getConnection();
        //connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);

        return connection;
    }

    /**
     * MySQL allows TIMESTAMP fields to have the value '0000-00-00 00:00:00',
     * which causes errors in import. If the user has not set the
     * zeroDateTimeBehavior property already, we set it for them to coerce
     * the type to null.
     */
    private void checkDateTimeBehavior() {
        final String ZERO_BEHAVIOR_STR = "zeroDateTimeBehavior";
        final String CONVERT_TO_NULL = "=convertToNull";

        // This starts with 'jdbc:mysql://' ... let's remove the 'jdbc:'
        // prefix so that java.net.URI can parse the rest of the line.
        String uriComponent = this.conString.substring(5);
        try {
            URI uri = new URI(uriComponent);
            String query = uri.getQuery(); // get the part after a '?'

            // If they haven't set the zeroBehavior option, set it to
            // squash-null for them.
            if (null == query) {
                conString = conString + "?" + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
                LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
            } else if (query.length() == 0) {
                conString = conString + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
                LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
            } else if (query.indexOf(ZERO_BEHAVIOR_STR) == -1) {
                if (!conString.endsWith("&")) {
                    conString = conString + "&";
                }
                conString = conString + ZERO_BEHAVIOR_STR + CONVERT_TO_NULL;
                LOG.info("Setting zero DATETIME behavior to convertToNull (mysql)");
            }

            LOG.debug("Rewriting connect string to " + conString);
        } catch (URISyntaxException use) {
            // Just ignore this. If we can't parse the URI, don't attempt
            // to add any extra flags to it.
            LOG.debug("mysql: Couldn't parse connect str in checkDateTimeBehavior: "
                    + use);
        }
    }

    @Override
    public void execAndPrint(String s) {
        // Override default execAndPrint() with a special version that forces
        // use of fully-buffered ResultSets (MySQLManager uses streaming ResultSets
        // in the default execute() method; but the execAndPrint() method needs to
        // issue overlapped queries for metadata.)

        ResultSet results = null;
        try {
            // Explicitly setting fetchSize to zero disables streaming.
            results = super.execute(s, 0);
        } catch (SQLException sqlE) {
            LOG.warn("Error executing statement: ", sqlE);
            release();
            return;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
        }

        PrintWriter pw = new PrintWriter(System.out, true);
        try {
            formatAndPrintResultSet(results, pw);
        } finally {
            pw.close();
        }
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
    public boolean supportsStagingForExport() {
        return true;
    }

    @Override
    public String[] getColumnNames(String tableName) {
        Connection c = null;
        Statement s = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<String>();
        String schema = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
        String listColumnsQuery = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA = '" + schema + "' "
                + "  AND TABLE_NAME = '" + tableName + "' ";

        try {
            c = getConnection();
            s = c.createStatement();
            rs = s.executeQuery(listColumnsQuery);
            while (rs.next()) {
                String columnName = rs.getString(1);
                String columnDataType = rs.getString(2);
                if (SupportedMysqlDataType.isSupported(columnDataType)) {
                    columns.add(columnName);
                }
            }
            c.commit();
        } catch (SQLException sqle) {
            try {
                if (c != null) {
                    c.rollback();
                }
            } catch (SQLException ce) {
                LOG.warn("Failed to rollback transaction", ce);
            }
            LOG.warn("Failed to list columns from query: "
                    + listColumnsQuery, sqle);
            throw new RuntimeException(sqle);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException re) {
                    LOG.warn("Failed to close resultset", re);
                }
            }
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException se) {
                    LOG.warn("Failed to close statement", se);
                }
            }
        }

        return columns.toArray(new String[columns.size()]);
    }

    @Override
    public String[] getColumnNamesForProcedure(String procedureName) {
        List<String> ret = new ArrayList<String>();
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null,
                    procedureName, null);
            if (null == results) {
                LOG.debug("Get Procedure Columns returns null");
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn) {
                        String name = results.getString("COLUMN_NAME");
                        ret.add(name);
                    }
                }
                String[] result = ret.toArray(new String[ret.size()]);
                LOG.debug("getColumnsNamesForProcedure returns "
                        + StringUtils.join(ret, ","));
                return result;
            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException e) {
            LOG.warn("Error reading procedure metadata: ", e);
            throw new RuntimeException("Can't fetch column names for procedure.", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
            return null;
        }
    }

    @Override
    public Map<String, Integer> getColumnTypesForProcedure(String procedureName) {
        Map<String, Integer> ret = new TreeMap<String, Integer>();
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null,
                    procedureName, null);
            if (null == results) {
                LOG.debug("getColumnTypesForProcedure returns null");
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn) {
                        // we don't care if we get several rows for the
                        // same ORDINAL_POSITION (e.g. like H2 gives us)
                        // as we'll just overwrite the entry in the map:
                        ret.put(
                                results.getString("COLUMN_NAME"),
                                results.getInt("DATA_TYPE"));
                    }
                }

                LOG.debug("Columns returned = " + StringUtils.join(ret.keySet(), ","));
                LOG.debug("Types returned = " + StringUtils.join(ret.values(), ","));

                return ret.isEmpty() ? null : ret;
            } finally {
                if (results != null) {
                    results.close();
                }
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LOG.warn("Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
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
                LOG.debug("getColumnTypesForProcedure returns null");
                return null;
            }

            try {
                while (results.next()) {
                    if (results.getInt("COLUMN_TYPE")
                            != DatabaseMetaData.procedureColumnReturn) {
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
                if (results != null) {
                    results.close();
                }
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LOG.warn("Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
            return null;
        }
    }

    @Override
    protected String getListDatabasesQuery() {
        return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
    }

    @Override
    protected String getSchemaQuery() {
        return "SELECT SCHEMA()";
    }

    private Map<String, String> colTypeNames;
    private static final int YEAR_TYPE_OVERWRITE = Types.SMALLINT;
}

