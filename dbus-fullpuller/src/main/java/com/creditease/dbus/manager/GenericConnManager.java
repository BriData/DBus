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
import com.creditease.dbus.dbaccess.DataSourceProvider;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Database manager that is connects to a generic JDBC-compliantdatabase;
 * Provide a general implementation of Druid connection pool
 */
public class GenericConnManager implements ConnManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String jdbcDriverClass;
    private Connection connection;
    protected DBConfiguration dbConfig;
    protected String connectString;
    protected Statement lastStatement;
    protected static Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();

    //统计 打开的连接数
    private static AtomicInteger totalRefCount = new AtomicInteger(0);
    private ThreadLocal<Integer> threadRefCountHolder = new ThreadLocal<>();


    public GenericConnManager(final String driverClass, final DBConfiguration dbConfig, String connectString) {
        this.dbConfig = dbConfig;
        this.connectString = connectString;
        this.jdbcDriverClass = driverClass;
        this.threadRefCountHolder.set(0);
    }

    @Override
    public Connection getConnection() throws Exception {
        String whoami = getWhoamI();
        if (null == this.connection) {
            this.connection = makeConnection();
            int totalCount = totalRefCount.incrementAndGet();
            this.threadRefCountHolder.set(threadRefCountHolder.get() + 1);
            int threadCount = this.threadRefCountHolder.get();
            logger.debug("new JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);
            return this.connection;
        } else {
            int totalCount = this.totalRefCount.get();
            int threadCount = this.threadRefCountHolder.get();
            logger.debug("get JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);

            return this.connection;
        }
    }

    @Override
    public void release() {
        if (null != this.lastStatement) {
            try {
                this.lastStatement.close();
            } catch (SQLException e) {
                LoggingUtils.logAll(logger, "Exception closing executed Statement: ", e);
            }
            this.lastStatement = null;
        }
    }

    @Override
    public void close() throws SQLException {
        String whoami = getWhoamI();
        if (this.connection != null) {
            this.connection.close();
            this.connection = null;

            int totalCount = this.totalRefCount.decrementAndGet();
            this.threadRefCountHolder.set(threadRefCountHolder.get() - 1);
            int threadCount = this.threadRefCountHolder.get();
            logger.debug("delete JDBCConnection [{}]  threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);
        } else {
            this.connection = null;

            int totalCount = totalRefCount.get();
            int threadCount = this.threadRefCountHolder.get();
            logger.debug("set null JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);
        }
    }

    @Override
    public String getDriverClass() {
        return this.jdbcDriverClass;
    }

    /**
     * 使用connection 返回一个 statment用于后续使用，改statement 由lastStatement 管理
     *
     * @param stmt
     * @return
     * @throws Exception
     */
    @Override
    public PreparedStatement prepareStatement(String stmt) throws Exception {
        release();
        PreparedStatement statement = this.getConnection().prepareStatement(stmt,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.lastStatement = statement;
        return statement;
    }

    public String getWhoamI() {
        return Thread.currentThread().getName() + "_" + Thread.currentThread().getId();
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
    private synchronized Connection makeConnection() throws Exception {
        Connection connection;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: " + driverClass);
        }
        String username = (String) (this.dbConfig.get(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY));
        String password = (String) (this.dbConfig.get(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY));

        DataSource ds = this.dataSourceMap.get(this.connectString);
        if (ds == null) {
            Properties props = getDataSourceProperties();
            if (username != null) {
                props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_USERNAME, username);
            }
            if (password != null) {
                props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_PASSWORD, password);
            }
            props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_URL, this.connectString);

            DataSourceProvider provider = new DruidDataSourceProvider(props);
            ds = provider.provideDataSource();
            this.dataSourceMap.put(this.connectString, ds);
        }
        connection = ds.getConnection();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);

        return connection;
    }

    private Properties getDataSourceProperties() {
        Properties properties = null;
        switch (this.jdbcDriverClass) {
            case "oracle.jdbc.driver.OracleDriver":
                properties = FullPullHelper.getFullPullProperties(FullPullConstants.ZK_NODE_NAME_ORACLE_CONF, true);
                break;
            case "com.mysql.jdbc.Driver":
                properties = FullPullHelper.getFullPullProperties(FullPullConstants.ZK_NODE_NAME_MYSQL_CONF, true);
                break;
            case "com.ibm.db2.jcc.DB2Driver":
                properties = FullPullHelper.getFullPullProperties(FullPullConstants.ZK_NODE_NAME_DB2_CONF, true);
                break;
            default:
                logger.error("the jdbcDriverClass :{} is wrong", jdbcDriverClass);
                throw new RuntimeException("the jdbcDriverClass is wrong");
        }
        return properties;
    }

}

