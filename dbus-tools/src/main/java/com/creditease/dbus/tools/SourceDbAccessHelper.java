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


package com.creditease.dbus.tools;

import com.creditease.dbus.commons.ConnectionProvider;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.tools.common.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by 201605240095 on 2016/7/25.
 */
public class SourceDbAccessHelper {
    private String zkServer;

    private String connStr;
    private String dbusUser;
    private String dbusPasswd;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Connection getMysqlConn() throws Exception {
        try {
            // Parse zookeeper
            zkServer = ConfUtils.loadZKProperties().getProperty(Constants.ZOOKEEPER_SERVERS);
            // Get a SQL Connection object
            Connection mysqlConnection = ConnectionProvider.getInstance().createMySQLConnection(zkServer);
            return mysqlConnection;
        } catch (Exception e) {
            logger.info("Exception was caught when setting up mysql connection.");
            e.printStackTrace();
            throw e;
        }
    }

    private Connection createOracleDBConnection() throws Exception {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            logger.error("Oracle JDBC Driver not found! ");
            e.printStackTrace();
            throw e;
        }
        try {
            Connection connection = DriverManager.getConnection(
                    connStr, dbusUser,
                    dbusPasswd);
            return connection;
        } catch (SQLException e) {
            logger.error("Connection to Oracle DB failed!");
            e.printStackTrace();
            throw e;
        }
    }

    private void setLoginInfo(String ds_name) throws Exception {


        String sqlQuery = "select slave_url, dbus_user, dbus_pwd " +
                "from t_dbus_datasource as d " +
                "where d.ds_name = ?";

        // Set conn_string, user, passed, ds_id
        Connection mysqlConnection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            mysqlConnection = getMysqlConn();
            stmt = mysqlConnection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            // TODO: May needs to be changed
            stmt.setString(1, ds_name);
            rs = stmt.executeQuery();

            while (rs.next()) {
                connStr = rs.getString("slave_url");
                dbusUser = rs.getString("dbus_user");
                dbusPasswd = rs.getString("dbus_pwd");
            }

        } catch (SQLException e) {
            logger.error("Exception caught when fetching login info from MySQL db. ");
            e.printStackTrace();
            throw e;
        } finally {
            closeRS(rs);
            closeStmt(stmt);
            closeConn(mysqlConnection);
            logger.info("Successfully fetched slave_url, dbus_user, and dbus_password used for accessing Oracle DB. ");
        }
    }

    private void closeRS(ResultSet rs) throws Exception {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("Exception caught when trying to close a ResultSet. ");
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void closeStmt(Statement stmt) throws Exception {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("Exception caught when trying to close a statement. ");
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void closeConn(Connection conn) throws Exception {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("Exception caught when close a connection. ");
            e.printStackTrace();
            throw e;
        }
    }

    public void querySourceDb(String dsName, String sql) {
        Connection oracleConnection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            setLoginInfo(dsName);

            oracleConnection = createOracleDBConnection();
            stmt = oracleConnection.prepareStatement(sql);

            stmt.setFetchSize(2500);
            logger.info("Using fetchSize for next query: {}", 2500);
            stmt.setQueryTimeout(3600);
            logger.info("Using queryTimeout 3600 seconds");

            rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            long count = 0;
            logger.info("********************************************");
            while (rs.next()) {
                count++;

                System.out.println(" ");
                String columns = "";
                for (int i = 1; i <= columnCount; i++) {
                    Object obj = rs.getObject(i);
                    columns += String.format("%s(%s): %s, ", rsmd.getColumnLabel(i),
                            rsmd.getColumnTypeName(i), obj != null ? obj.toString() : null);
                }
                logger.info("{}:{}", count, columns);
            }
            logger.info("********************************************");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                closeRS(rs);
                closeStmt(stmt);
                closeConn(oracleConnection);
            } catch (Exception e) {
                logger.info("Close con/statement encountered exception.", e);
            }
            logger.info("Query finished.");
        }
    }

    public static void main(String[] args) throws Exception {
        SourceDbAccessHelper helper = new SourceDbAccessHelper();
        System.out.println(args.length);
        if (args.length != 2) {
            System.out.println("Please provide legal parameters. Example: p2p \"select * from t_foo_bar\"");
            return;
        }
        helper.querySourceDb(args[0], args[1]);
    }
}
