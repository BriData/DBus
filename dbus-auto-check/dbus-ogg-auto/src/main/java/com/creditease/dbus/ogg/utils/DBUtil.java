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


package com.creditease.dbus.ogg.utils;

import java.sql.*;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class DBUtil {
    private DBUtil() {

    }

    public static Connection getConn(String url, String name, String pwd) throws Exception {
        Connection conn = null;
        String driver = "oracle.jdbc.driver.OracleDriver"; //驱动
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, name, pwd);
            return conn;
        } catch (ClassNotFoundException e) {
            System.out.println("DBUtil getConn: driver load error.");
            throw e;
        } catch (SQLException e) {
            System.out.println("DBUtil getConn: create connection error.");
            throw e;
        }

    }

    public static void close(Object obj) throws Exception {
        if (obj == null) return;
        try {
            if (obj instanceof PreparedStatement) ((PreparedStatement) obj).close();
            if (obj instanceof ResultSet) ((ResultSet) obj).close();
            if (obj instanceof Connection) ((Connection) obj).close();
        } catch (SQLException e) {
            System.out.println("DBUtil close method error.");
            throw e;
        }
    }
}
