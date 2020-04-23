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

import com.creditease.dbus.bean.SinkerTopologyTable;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * dbusmgr数据库相关查询
 */
public class DBHelper {
    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    private static DataSource dataSource = null;

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

    public static List<SinkerTopologyTable> querySinkerTables(Properties properties, String sinkerName) {
        List<SinkerTopologyTable> sinkerTopologySchemas = new ArrayList<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = getDBusMgrConnection(properties);
            String sql = " SELECT" +
                    " 	t.id," +
                    " 	t.sinker_topo_id," +
                    " 	t.sinker_name," +
                    " 	t.ds_id," +
                    " 	t.ds_name," +
                    " 	m.alias," +
                    " 	t.schema_id," +
                    " 	t.schema_name," +
                    " 	s.target_topic," +
                    " 	t.table_id," +
                    " 	t.table_name," +
                    " 	t.description," +
                    " 	t.update_time" +
                    " FROM" +
                    " 	t_sinker_topo_table t" +
                    " JOIN t_sinker_topo_schema s ON s.schema_id = t.schema_id" +
                    " LEFT JOIN t_name_alias_mapping m ON t.ds_id = m.name_id" +
                    " AND m.type = 2" +
                    " WHERE" +
                    " 	t.sinker_name = ?";
            logger.info("sql:{},param:{}", sql, sinkerName);
            ps = conn.prepareStatement(sql);
            ps.setString(1, sinkerName);
            rs = ps.executeQuery();
            while (rs.next()) {
                SinkerTopologyTable sinkerTopologySchema = new SinkerTopologyTable();
                sinkerTopologySchema.setId(rs.getInt("id"));
                sinkerTopologySchema.setSinkerTopoId(rs.getInt("sinker_topo_id"));
                sinkerTopologySchema.setSinkerName(rs.getString("sinker_name"));
                sinkerTopologySchema.setDsId(rs.getInt("ds_id"));
                sinkerTopologySchema.setDsName(rs.getString("ds_name"));
                sinkerTopologySchema.setAlias(rs.getString("alias"));
                sinkerTopologySchema.setSchemaId(rs.getInt("schema_id"));
                sinkerTopologySchema.setSchemaName(rs.getString("schema_name"));
                sinkerTopologySchema.setTargetTopic(rs.getString("target_topic"));
                sinkerTopologySchema.setTableId(rs.getInt("table_id"));
                sinkerTopologySchema.setTableName(rs.getString("table_name"));
                sinkerTopologySchema.setDescription(rs.getString("description"));
                sinkerTopologySchema.setUpdateTime(rs.getDate("update_time"));
                sinkerTopologySchemas.add(sinkerTopologySchema);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return sinkerTopologySchemas;
    }

}
