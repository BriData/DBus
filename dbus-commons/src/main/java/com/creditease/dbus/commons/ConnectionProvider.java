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


package com.creditease.dbus.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionProvider {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static ConnectionProvider instance = new ConnectionProvider();

    public static ConnectionProvider getInstance() {
        return instance;
    }

    public Connection createMySQLConnection(String zkServer) throws Exception {

        Connection connection = null;
        // Connect to Zookeeper

        // 1. Connect to database
        // 获得mysql properties
        String sqlPath = Constants.COMMON_ROOT + "/" + Constants.MYSQL_PROPERTIES;
        Properties mysqlProps = ZkUtils.getProperties(zkServer, sqlPath);

        // 2. Set up jdbc driver
        try {
            Class.forName(mysqlProps.getProperty("driverClassName"));
        } catch (ClassNotFoundException e) {
            logger.error("MySQL JDBC Driver not found. ");
            e.printStackTrace();
            throw e;
        }

        // Get information about database from mysqlProps for connecting DB
        String dbAddr = mysqlProps.getProperty("url");
        String userName = mysqlProps.getProperty("username");
        String password = mysqlProps.getProperty("password");

        try {
            connection = DriverManager
                    .getConnection(dbAddr, userName, password);
            return connection;
        } catch (SQLException e) {
            logger.error("Connection to MySQL DB failed!");
            e.printStackTrace();
            throw e;
        } finally {
            if (connection != null) {
                logger.info("Make MySQL DB Connection successful. ");
            } else {
                logger.error("Failed to make MySQL DB Connection!");
            }
        }
    }
}
