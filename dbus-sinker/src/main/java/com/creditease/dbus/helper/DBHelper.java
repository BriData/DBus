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


package com.creditease.dbus.helper;

import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * dbusmgr数据库相关查询
 */
public class DBHelper {
    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    private static DataSource dataSource = null;
    private static Map<Integer, String> resultTopicMap = null;

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
    private static synchronized Connection getDBusMgrConnection(Properties mysqlProps) throws Exception {
        Connection connection;
        if (dataSource == null) {
            String driverClass = mysqlProps.getProperty("driverClassName");
            try {
                Class.forName(driverClass);
            } catch (ClassNotFoundException cnfe) {
                throw new RuntimeException("Could not load db driver class: " + driverClass);
            }
            mysqlProps.setProperty("username", mysqlProps.getProperty("username"));
            mysqlProps.setProperty("password", mysqlProps.getProperty("password"));
            mysqlProps.setProperty("url", mysqlProps.getProperty("url"));
            DruidDataSourceProvider provider = new DruidDataSourceProvider(mysqlProps);
            dataSource = provider.provideDataSource();
        }
        connection = dataSource.getConnection();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * 统一资源关闭处理
     *
     * @param conn
     * @param ps
     * @param rs
     * @return
     * @throws SQLException
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("DBHelper close resource exception", e);
        }
    }


    public static Map<String, String> queryAliasMapping(Properties mysqlProps) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> aliasMapping = new HashMap<>();
        try {
            conn = getDBusMgrConnection(mysqlProps);
            String sql = "SELECT CONCAT(map.alias,'.',s.schema_name) alias,ds.ds_name " +
                    "FROM t_dbus_datasource ds JOIN t_name_alias_mapping map ON ds.id = map.name_id " +
                    "JOIN t_data_schema s ON map.name_id = s.ds_id " +
                    "WHERE map.type = 2";
            logger.info("[load dsName alias] sql :{}", sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                aliasMapping.put(rs.getString("alias"), rs.getString("ds_name"));
            }
        } catch (Exception e) {
            logger.error("[load dsName alias]", e);
        } finally {
            close(conn, ps, rs);
        }
        logger.info("[load dsName alias] {}", aliasMapping);
        return aliasMapping;
    }
}
