/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.dbus.commons.ConnectionProvider;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.tools.common.ConfUtils;

/**
 * Created by 201605240095 on 2016/7/25.
 */
public class SourceDbAccessHelper {
    private String zkServer;

    private String connStr;
    private String dbusUser;
    private String dbusPasswd;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Connection getMysqlConn () throws Exception {
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

    private Connection createOracleDBConnection () throws Exception {
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

    private void setLoginInfo (Connection mysqlConnection, String ds_name) throws Exception {
        String sqlQuery = "select master_url, dbus_user, dbus_pwd " +
                "from t_dbus_datasource as d " +
                "where d.ds_name = ?";

        // Set conn_string, user, passed, ds_id
        PreparedStatement stmt = null;
        try {
            stmt = mysqlConnection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            // TODO: May needs to be changed
            stmt.setString(1, ds_name);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                connStr = rs.getString("master_url");
                dbusUser = rs.getString("dbus_user");
                dbusPasswd = rs.getString("dbus_pwd");
            }

        } catch (SQLException e) {
            logger.error("Exception caught when fetching login info from MySQL db. ");
            e.printStackTrace();
            throw e;
        } finally {
            closeStmt(stmt);
            logger.info("Successfully fetched master_url, dbus_user, and dbus_password used for accessing Oracle DB. ");
        }
    }

    private void closeStmt (Statement stmt) throws Exception {
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

    private void closeConn (Connection conn) throws Exception {
        try {
            if (conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("Exception caught when close a connection. ");
            e.printStackTrace();
            throw e;
        }
    }

    public void querySourceDb(String dsName, String sql) {
     	 Connection mysqlConn = null;
         Connection oracleConnection = null;
         PreparedStatement stmt = null;
         ResultSet rs = null;
         try {
             mysqlConn = getMysqlConn(); 
             setLoginInfo(mysqlConn, dsName);
             oracleConnection = createOracleDBConnection();
             stmt = oracleConnection.prepareStatement(sql); 
             rs = stmt.executeQuery();
             ResultSetMetaData rsmd = rs.getMetaData();
             int columnCount = rsmd.getColumnCount();
             while(rs.next()){
            	 for (int i = 1; i <= columnCount; i++) { 
            		 logger.info("{}({})-------------{}.",rsmd.getColumnLabel(i),rsmd.getColumnTypeName(i),rs.getObject(i));
            	 }
            	 logger.info("********************************************");
             }
         } catch (Exception e) {
             logger.error("Exception!",e);
         } finally {
             try {
            	 closeConn(mysqlConn);
				 closeConn(oracleConnection);
                 closeStmt(stmt);
             } catch (Exception e) {
            	 logger.info("Close con/statement encountered exception.",e);
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
