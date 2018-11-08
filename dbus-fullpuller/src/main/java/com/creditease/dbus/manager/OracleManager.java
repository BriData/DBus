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
import com.creditease.dbus.common.utils.LoggingUtils;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.SupportedOraDataType;
import com.creditease.dbus.dbaccess.DataSourceProvider;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

/**
 * Manages connections to Oracle databases.
 * Requires the Oracle JDBC driver.
 */
public class OracleManager
        extends GenericJdbcManager {

    private static Logger LOG = LoggerFactory.getLogger(OracleManager.class);

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
    public static final String QUERY_LIST_DATABASES =
            "SELECT USERNAME FROM DBA_USERS";

    /**
     * Query to list all tables visible to the current user. Note that this list
     * does not identify the table owners which is required in order to
     * ensure that the table can be operated on for import/export purposes.
     */
    public static final String QUERY_LIST_TABLES =
            "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?";

    /**
     * Query to list all columns of the given table. Even if the user has the
     * privileges to access table objects from another schema, this query will
     * limit it to explore tables only from within the active schema.
     */
    public static final String QUERY_COLUMNS_FOR_TABLE =
            "SELECT COLUMN_NAME columnName, DATA_TYPE columnTypeName FROM ALL_TAB_COLUMNS WHERE "
                    + "OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID";
    ;

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
//  public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
//          "SELECT ALL_IND_COLUMNS.COLUMN_NAME FROM ALL_IND_COLUMNS, "
//                  + "ALL_INDEXES WHERE ALL_IND_COLUMNS.TABLE_NAME = "
//                  + "ALL_INDEXES.TABLE_NAME AND "
//                  + "ALL_IND_COLUMNS.TABLE_NAME = ? AND "
//                  + "ALL_INDEXES.TABLE_OWNER = ? AND UNIQUENESS = 'UNIQUE'";


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
//  public static final String QUERY_INDEXED_COL_FOR_TABLE =
//          "SELECT ALL_IND_COLUMNS.COLUMN_NAME FROM ALL_IND_COLUMNS, "
//           + "ALL_INDEXES WHERE ALL_IND_COLUMNS.TABLE_NAME = "
//           + "ALL_INDEXES.TABLE_NAME AND "
//           + "ALL_IND_COLUMNS.TABLE_NAME = ? AND "
//           + "ALL_INDEXES.TABLE_OWNER = ? AND UNIQUENESS = 'NONUNIQUE'";

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
    public static final String QUERY_GET_SESSIONUSER =
            "SELECT USER FROM DUAL";

    // driver class to ensure is loaded when making db connection.
    public static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";

    // Configuration key to use to set the session timezone.
    public static final String ORACLE_TIMEZONE_KEY = "oracle.sessionTimeZone";

    // Oracle XE does a poor job of releasing server-side resources for
    // closed connections. So we actually want to cache connections as
    // much as possible. This is especially important for JUnit tests which
    // may need to make 60 or more connections (serially), since each test
    // uses a different OracleManager instance.
    @Deprecated
    private static class ConnCache {
        private Logger LOG = LoggerFactory.getLogger(getClass());

        private static class CacheKey {
            private final String connectString;
            private final String username;

            public CacheKey(String connect, String user) {
                this.connectString = connect;
                this.username = user; // note: may be null.
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof CacheKey) {
                    CacheKey k = (CacheKey) o;
                    if (null == username) {
                        return k.username == null && k.connectString.equals(connectString);
                    } else {
                        return k.username.equals(username)
                                && k.connectString.equals(connectString);
                    }
                } else {
                    return false;
                }
            }

            @Override
            public int hashCode() {
                if (null == username) {
                    return connectString.hashCode();
                } else {
                    return username.hashCode() ^ connectString.hashCode();
                }
            }

            @Override
            public String toString() {
                return connectString + "/" + username;
            }
        }

        private Map<CacheKey, Connection> connectionMap;

        public ConnCache() {
            LOG.debug("Instantiated new connection cache.");
            connectionMap = new HashMap<CacheKey, Connection>();
        }

        /**
         * @return a Connection instance that can be used to connect to the
         * given database, if a previously-opened connection is available in
         * the cache. Returns null if none is available in the map.
         */
        public synchronized Connection getConnection(String connectStr,
                                                     String username) throws SQLException {
            CacheKey key = new CacheKey(connectStr, username);
            Connection cached = connectionMap.get(key);
            if (null != cached) {
                connectionMap.remove(key);
                if (cached.isReadOnly()) {
                    // Read-only mode? Don't want it.
                    cached.close();
                }

                if (cached.isClosed()) {
                    // This connection isn't usable.
                    return null;
                }

                cached.rollback(); // Reset any transaction state.
                cached.clearWarnings();

                LOG.debug("Got cached connection for " + key);
            }

            return cached;
        }

        /**
         * Returns a connection to the cache pool for future use. If a connection
         * is already cached for the connectstring/username pair, then this
         * connection is closed and discarded.
         */
        public synchronized void recycle(String connectStr, String username,
                                         Connection conn) throws SQLException {

            CacheKey key = new CacheKey(connectStr, username);
            Connection existing = connectionMap.get(key);
            if (null != existing) {
                // Cache is already full for this entry.
                LOG.debug("Discarding additional connection for " + key);
                conn.close();
                return;
            }

            // Put it in the map for later use.
            LOG.debug("Caching released connection for " + key);
            connectionMap.put(key, conn);
        }

        @Override
        protected synchronized void finalize() throws Throwable {
            for (Connection c : connectionMap.values()) {
                c.close();
            }

            super.finalize();
        }
    }

//  private static final ConnCache CACHE;
//  static {
//    CACHE = new ConnCache();
//  }

    public OracleManager(final DBConfiguration opts, String conString) {
        super(DRIVER_CLASS, opts, conString);
        Object scn = opts.get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
        if (scn == null || (Long) scn == 0) {
            LOG.warn("Not set SCN yet!");
//            try {
//                scn = getCurrentScn(getConnection());
//            }
//            catch (SQLException e) {
//            }
        }
//        opts.set(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN, scn);
//        LOG.info("Performing a consistent read using SCN: " + scn);


    }

//  public void close() throws SQLException, Exception {
//    release(); // Release any open statements associated with the connection.
//    if (hasOpenConnection()) {
//      // Release our open connection back to the cache.
//        CACHE.recycle((String)(options.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY)), (String)(options.get(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY)),
//                      getConnection());
//      discardConnection(false);
//    }
//  }

    protected String getColNamesQuery(String tableName) {
        // SqlManager uses "tableName AS t" which doesn't work in Oracle.
        String query = "SELECT t.* FROM " + escapeTableName(tableName)
                + " t WHERE 1=0";

        LOG.debug("Using column names query: " + query);
        return query;
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
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }
        String username = (String) (options.get(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY));
        String password = (String) (options.get(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY));
//    String connectStr = (String)(options.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
//    if(!readOnly){
//        connectStr = (String)(options.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE));
//    }
    
    /*try {
      connection = CACHE.getConnection(this.conString, username);
    } catch (SQLException e) {
      connection = null;
      LOG.debug("Cached connecion has expired.");
    }*/

        // if (null == connection) {
        DataSource ds = dataSourceMap.get(this.conString);
        if (ds == null) {
            Properties props = FullPullHelper.getFullPullProperties(DataPullConstants.ZK_NODE_NAME_ORACLE_CONF, true);
            if (username != null) {
                props.setProperty(Constants.DB_CONF_PROP_KEY_USERNAME, username);
            }
            if (password != null) {
                props.setProperty(Constants.DB_CONF_PROP_KEY_PASSWORD, password);
            }
            props.setProperty(Constants.DB_CONF_PROP_KEY_URL, this.conString);
            
/*          props.setProperty("oracle.jdbc.V8Compatible", "true");  
 * 			props.setProperty("maxActive", "15");
            props.setProperty("minIdle", "3");
            props.setProperty("timeBetweenEvictionRunsMillis", "60000");
            props.setProperty("minEvictableIdleTimeMillis", "300000");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("removeAbandoned", "true");
            props.setProperty("removeAbandonedTimeout", "1800");*/

            DataSourceProvider provider = new DruidDataSourceProvider(props);
            ds = provider.provideDataSource();
            dataSourceMap.put(this.conString, ds);
        }
        connection = ds.getConnection();
        // }
//    if (null == connection) {
//      // Couldn't pull one from the cache. Get a new one.
//      LOG.debug("Creating a new connection for "
//              + connectStr + ", using username: " + username);
//      String connectionParamsStr =
//              (String)(options.get(DBConfiguration.CONNECTION_PARAMS_PROPERTY));
//      Properties connectionParams = DBConfiguration.propertiesFromString(connectionParamsStr);
//      if (connectionParams != null && connectionParams.size() > 0) {
//        LOG.debug("User specified connection params. "
//                  + "Using properties specific API for making connection.");
//        Properties props = new Properties();
//        if (username != null) {
//          props.put("user", username);
//        }
//
//        if (password != null) {
//          props.put("password", password);
//        }
//
//        props.putAll(connectionParams);
//        connection = DriverManager.getConnection(connectStr, props);
//        
//      } else {
//        LOG.debug("No connection paramenters specified. "
//                + "Using regular API for making connection.");
//        if (username == null) {
//          connection = DriverManager.getConnection(connectStr);
//        } else {
//          connection = DriverManager.getConnection(
//                              connectStr, username, password);
//        }
//      }
//    }

        // We only use this for metadata queries. Loosest semantics are okay.
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        // Setting session time zone
//    setSessionTimeZone(connection);

        // Rest of the code expects that the connection will have be running
        // without autoCommit, so we need to explicitly set it to false. This is
        // usually done directly by SqlManager in the makeConnection method, but
        // since we are overriding it, we have to do it ourselves.
        connection.setAutoCommit(false);

        return connection;
    }

    public static String getSessionUser(Connection conn) {
        Statement stmt = null;
        ResultSet rset = null;
        String user = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rset = stmt.executeQuery(QUERY_GET_SESSIONUSER);

            if (rset.next()) {
                user = rset.getString(1);
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }
        }
        if (user == null) {
            throw new RuntimeException("Unable to get current session user");
        }
        return user;
    }

    /**
     * Set session time zone.
     *
     * @param conn Connection object
     * @throws SQLException instance
     */
    private void setSessionTimeZone(Connection conn) throws SQLException {
        // Need to use reflection to call the method setSessionTimeZone on the
        // OracleConnection class because oracle specific java libraries are not
        // accessible in this context.
        Method method;
        try {
            method = conn.getClass().getMethod(
                    "setSessionTimeZone", new Class[]{String.class});
        } catch (Exception ex) {
            LOG.error("Could not find method setSessionTimeZone in "
                    + conn.getClass().getName(), ex);
            // rethrow SQLException
            throw new SQLException(ex);
        }

        // Need to set the time zone in order for Java to correctly access the
        // column "TIMESTAMP WITH LOCAL TIME ZONE".  The user may have set this in
        // the configuration as 'oracle.sessionTimeZone'.
        String clientTimeZoneStr = (String) (options.get(ORACLE_TIMEZONE_KEY));
        clientTimeZoneStr = clientTimeZoneStr == null ? "GMT" : clientTimeZoneStr;
        try {
            method.setAccessible(true);
            method.invoke(conn, clientTimeZoneStr);
            LOG.info("Time zone has been set to " + clientTimeZoneStr);
        } catch (Exception ex) {
            LOG.warn("Time zone " + clientTimeZoneStr
                    + " could not be set on Oracle database.");
            LOG.info("Setting default time zone: GMT");
            try {
                // Per the documentation at:
                // http://download-west.oracle.com/docs/cd/B19306_01
                //     /server.102/b14225/applocaledata.htm#i637736
                // The "GMT" timezone is guaranteed to exist in the available timezone
                // regions, whereas others (e.g., "UTC") are not.
                method.invoke(conn, "GMT");
            } catch (Exception ex2) {
                LOG.error("Could not set time zone for oracle connection", ex2);
                // rethrow SQLException
                throw new SQLException(ex);
            }
        }
    }

    @Override
    public ResultSet readTable(String tableName, String[] columns)
            throws SQLException, Exception {
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

        String sqlCmd = sb.toString();
        LOG.debug("Reading table with command: " + sqlCmd);
        return execute(sqlCmd);
    }

    private Map<String, String> columnTypeNames;

    /**
     * Resolve a database-specific type to the Java type that should contain it.
     *
     * @param tableName table name
     * @param colName   column name
     * @return the name of a Java type to hold the sql datatype, or null if none.
     */
    private String toDbSpecificJavaType(String tableName, String colName) {
        if (columnTypeNames == null) {
            columnTypeNames = getColumnTypeNames(tableName, (String) (options.get(DBConfiguration.INPUT_DB_PROCEDURE_CALL_NAME)),
                    (String) (options.get(DBConfiguration.INPUT_QUERY)));
        }

        String colTypeName = columnTypeNames.get(colName);
        if (colTypeName != null) {
            if (colTypeName.equalsIgnoreCase("BINARY_FLOAT")) {
                return "Float";
            }
            if (colTypeName.equalsIgnoreCase("FLOAT")) {
                return "Float";
            }
            if (colTypeName.equalsIgnoreCase("BINARY_DOUBLE")) {
                return "Double";
            }
            if (colTypeName.equalsIgnoreCase("DOUBLE")) {
                return "Double";
            }
            if (colTypeName.toUpperCase().startsWith("TIMESTAMP")) {
                return "java.sql.Timestamp";
            }
        }
        return null;
    }

    /**
     * Resolve a database-specific type to the Hive type that should contain it.
     *
     * @param tableName table name
     * @param colName   column name
     * @return the name of a Hive type to hold the sql datatype, or null if none.
     */
    private String toDbSpecificHiveType(String tableName, String colName) {
        if (columnTypeNames == null) {
            columnTypeNames = getColumnTypeNames(tableName, (String) (options.get(DBConfiguration.INPUT_DB_PROCEDURE_CALL_NAME)),
                    (String) (options.get(DBConfiguration.INPUT_QUERY)));
        }
        LOG.debug("Column Types and names returned = ("
                + StringUtils.join(columnTypeNames.keySet(), ",")
                + ")=>("
                + StringUtils.join(columnTypeNames.values(), ",")
                + ")");

        String colTypeName = columnTypeNames.get(colName);
        if (colTypeName != null) {
            if (colTypeName.equalsIgnoreCase("BINARY_FLOAT")) {
                return "FLOAT";
            }
            if (colTypeName.equalsIgnoreCase("BINARY_DOUBLE")) {
                return "DOUBLE";
            }
            if (colTypeName.toUpperCase().startsWith("TIMESTAMP")) {
                return "STRING";
            }
        }
        return null;
    }

    /**
     * Return java type for SQL type.
     *
     * @param tableName  table name
     * @param columnName column name
     * @param sqlType    sql type
     * @return java type
     */
    @Override
    public String toJavaType(String tableName, String columnName, int sqlType) {
        String javaType = super.toJavaType(tableName, columnName, sqlType);
        if (javaType == null) {
            javaType = toDbSpecificJavaType(tableName, columnName);
        }
        return javaType;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    protected String getCurTimestampQuery() {
        return "SELECT SYSDATE FROM dual";
    }

    @Override
    public String timestampToQueryString(Timestamp ts) {
        return "TO_TIMESTAMP('" + ts + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
    }

    @Override
    public String datetimeToQueryString(String datetime, int columnType) {
        if (columnType == Types.TIMESTAMP) {
            return "TO_TIMESTAMP('" + datetime + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
        } else if (columnType == Types.DATE) {
            // converting timestamp of the form 2012-11-11 11:11:11.00 to
            // date of the form 2011:11:11 11:11:11
            datetime = datetime.split("\\.")[0];
            return "TO_DATE('" + datetime + "', 'YYYY-MM-DD HH24:MI:SS')";
        } else {
            String msg = "Column type is neither timestamp nor date!";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    @Override
    public boolean supportsStagingForExport() {
        return true;
    }

    /**
     * The concept of database in Oracle is mapped to schemas. Each schema
     * is identified by the corresponding username.
     */
    @Override
    public String[] listDatabases() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rset = null;
        List<String> databases = new ArrayList<String>();

        try {
            conn = getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rset = stmt.executeQuery(QUERY_LIST_DATABASES);

            while (rset.next()) {
                databases.add(rset.getString(1));
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }

            if (e.getErrorCode() == ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST) {
                LOG.error("The catalog view DBA_USERS was not found. "
                        + "This may happen if the user does not have DBA privileges. "
                        + "Please check privileges and try again.");
                LOG.debug("Full trace for ORA-00942 exception", e);
            } else {
                LoggingUtils.logAll(LOG, "Failed to list databases", e);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }

//      try {
//        close();
//      } catch (SQLException ex) {
//        LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//      }
//    catch (Exception e) {
//        // TODO Auto-generated catch block
//        LOG.error(e.getMessage(),e);
//    }
        }

        return databases.toArray(new String[databases.size()]);
    }

    @Override
    public String[] listTables() {
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet rset = null;
        List<String> tables = new ArrayList<String>();
        String tableOwner = null;


        try {
            conn = getConnection();
            tableOwner = getSessionUser(conn);
            pStmt = conn.prepareStatement(QUERY_LIST_TABLES,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, tableOwner);

            rset = pStmt.executeQuery();

            while (rset.next()) {
                tables.add(rset.getString(1));
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to list tables", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
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

//      try {
//        close();
//      } catch (SQLException ex) {
//        LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//      }
//    catch (Exception e) {
//        // TODO Auto-generated catch block
//        LOG.error(e.getMessage(),e);
//    }
        }

        return tables.toArray(new String[tables.size()]);
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
                        int index = results.getInt("ORDINAL_POSITION");
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
                String[] result = ret.toArray(new String[ret.size()]);
                LOG.debug("getColumnsNamesForProcedure returns "
                        + StringUtils.join(ret, ","));
                return result;
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

    @Override
    public Map<String, Integer>
    getColumnTypesForProcedure(String procedureName) {
        Map<String, Integer> ret = new TreeMap<String, Integer>();
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
                        int index = results.getInt("ORDINAL_POSITION");
                        if (index < 0) {
                            continue; // actually the return type
                        }
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
                results.close();
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LoggingUtils.logAll(LOG, "Error reading primary key metadata: "
                    + sqlException.toString(), sqlException);
            return null;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
            return null;
        }
    }

    @Override
    public Map<String, String>
    getColumnTypeNamesForProcedure(String procedureName) {
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
                            != DatabaseMetaData.procedureColumnReturn) {
                        int index = results.getInt("ORDINAL_POSITION");
                        if (index < 0) {
                            continue; // actually the return type
                        }
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
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
            return null;
        }
    }

    @Override
    public String[] getColumnNames(String tableName) {
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet rset = null;
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

            if (tableOwner == null) {
                tableOwner = getSessionUser(conn);
            }

            pStmt = conn.prepareStatement(QUERY_COLUMNS_FOR_TABLE, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, tableOwner);
            pStmt.setString(2, shortTableName);

            //pStmt = conn.prepareStatement("select * from " + tableName + " where rownum<=1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            rset = pStmt.executeQuery();
            while (rset.next()) {
                String columnTypeName = rset.getString("columnTypeName");
                if (columnTypeName != null && SupportedOraDataType.isSupported(columnTypeName)) {
                    columns.add(rset.getString("columnName"));
                }
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to list columns", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
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

//            try {
//                close();
//            }
//            catch (SQLException ex) {
//                LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//            }
//            catch (Exception e) {
//                // TODO Auto-generated catch block
//                LOG.error(e.getMessage(),e);
//            }
        }

        return columns.toArray(new String[columns.size()]);
    }

    @Override
    public String getPrimaryKey(String tableName) {
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet rset = null;
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

            if (tableOwner == null) {
                tableOwner = getSessionUser(conn);
            }

            pStmt = conn.prepareStatement(QUERY_PRIMARY_KEY_FOR_TABLE,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, shortTableName);
            pStmt.setString(2, tableOwner);
            rset = pStmt.executeQuery();

            while (rset.next()) {
                columns.add(rset.getString(1));
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to list columns", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
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

            try {
                close();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.error(e.getMessage(),e);
            }
        }

        if (columns.size() == 0) {
            // Table has no primary key
            return null;
        }

        if (columns.size() > 1) {
            // The primary key is multi-column primary key. Warn the user.
            // TODO select the appropriate column instead of the first column based
            // on the datatype - giving preference to numerics over other types.
            LOG.warn("The table " + tableName + " "
                    + "contains a multi-column primary key. Sqoop will default to "
                    + "the column " + columns.get(0) + " only for this job.");
        }

        return columns.get(0);
    }

    /**
     * Build the boundary query for the column of the result set created by
     * the given query.
     *
     * @param splitByCol     column name whose boundaries we're interested in.
     * @param sanitizedQuery sub-query used to create the result set.
     * @return input boundary query as a string
     */
    @Override
    public String getInputBoundsQuery(String splitByCol, String sanitizedQuery) {
        /*
         * The default input bounds query generated by DataDrivenImportJob
         * is of the form:
         *  SELECT MIN(splitByCol), MAX(splitByCol) FROM (sanitizedQuery) AS t1
         *
         * This works for most databases but not Oracle since Oracle does not
         * allow the use of "AS" to project the subquery as a table. Instead the
         * correct format for use with Oracle is as follows:
         *  SELECT MIN(splitByCol), MAX(splitByCol) FROM (sanitizedQuery) t1
         */
        return "SELECT MIN(" + splitByCol + "), MAX(" + splitByCol + ") FROM ("
                + sanitizedQuery + ") t1";
    }

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

    public MetaWrapper queryMetaInOriginalDb() {
        MetaWrapper metaInOriginalDb = new MetaWrapper();
        Connection conn = null;
        PreparedStatement pStmt = null;
        ResultSet rs = null;

        String schema = options.getString(DBConfiguration.INPUT_SCHEMA_PROPERTY);
        String table = options.getString(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY);
        // 对于系列表，任取其中一个表来获取Meta信息。此处取第一个表。
        if (table.indexOf(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            table = table.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }
        if (table.indexOf(".") != -1) {
            table = table.split("\\.")[1];
        }
        // 如果meta变了，scn即刻失效，抛异常,会返回false，符合预期。所以这里查meta不用带scn号，来尝试取“某个时刻”的meta。
        String sql = "select t1.owner,\n" +
                "       t1.table_name,\n" +
                "       t1.column_name,\n" +
                "       t1.column_id,\n" +
                "       t1.data_type,\n" +
                "       t1.data_length,\n" +
                "       t1.data_precision,\n" +
                "       t1.data_scale,\n" +
                "       t1.nullable,\n" +
                "       systimestamp,\n" +
                "       decode(t2.is_pk, 1, 'Y', 'N') is_pk,\n" +
                "       decode(t2.position, null, -1, t2.position) pk_position\n" +
                "  from (select t.*\n" +
                "          from dba_tab_columns t\n" +
                "         where t.owner = '" + schema.toUpperCase() + "'\n" +
                "           and t.table_name = '" + table.toUpperCase() + "') t1\n" +
                "  left join (select cu.table_name,cu.column_name, cu.position, 1 as is_pk\n" +
                "               from dba_cons_columns cu, dba_constraints au\n" +
                "              where cu.constraint_name = au.constraint_name and cu.owner = au.owner \n" +
                "                and au.constraint_type = 'P'\n" +
                "                and au.table_name = '" + table.toUpperCase() + "'\n" +
                "                and au.owner = '" + schema.toUpperCase() + "') t2 on (t1.column_name = t2.column_name and t1.table_name = t2.table_name)\n";

        LOG.info("[Oracle manager] Meta query sql is {}.", sql);

        try {
            conn = getConnection();
            pStmt = conn.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = pStmt.executeQuery();
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
//                cell.setDdlTime(rs.getTimestamp("ddl_time"));
                cell.setIsPk(rs.getString("is_pk"));
                cell.setPkPosition(rs.getInt("pk_position"));
                metaInOriginalDb.addMetaCell(cell);
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to query meta from original DB.", e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.warn("Query Meta In Original Db encountered Excetpion", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
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
            // connection的关闭在manager用完后有manager.close()统一做
//        try {
//          close();
//        } catch (SQLException ex) {
//          LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//        }
//        catch (Exception e) {
//            // TODO Auto-generated catch block
//            LOG.error(e.getMessage(),e);
//        }
        }

        return metaInOriginalDb;
    }
}
