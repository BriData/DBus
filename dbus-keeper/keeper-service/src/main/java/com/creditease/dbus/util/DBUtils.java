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


package com.creditease.dbus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by Administrator on 2018/8/1.
 */
public class DBUtils {

    private static Logger logger = LoggerFactory.getLogger(DBUtils.class);

    private DBUtils() {
    }

    public static Connection getConn(String url, String name, String password) throws Exception {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, name, password);
        } catch (ClassNotFoundException e) {
            logger.error("DBUtils getConn method error.", e);
            throw e;
        } catch (SQLException e) {
            logger.error("DBUtils getConn method error.", e);
            throw e;
        }
        return conn;
    }

    public static void close(Object obj) {
        if (obj == null) return;
        try {
            if (obj instanceof PreparedStatement) ((PreparedStatement) obj).close();
            if (obj instanceof ResultSet) ((ResultSet) obj).close();
            if (obj instanceof Connection) ((Connection) obj).close();
        } catch (SQLException e) {
            logger.error("DBUtils close method error.", e);
        }
    }

}
