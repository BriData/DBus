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


package com.creditease.dbus.heartbeat.dao.impl;

import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.dao.IDbusDataDao;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.type.MaasMessage;
import com.creditease.dbus.heartbeat.type.MaasMessage.DataSource;
import com.creditease.dbus.heartbeat.type.MaasMessage.DataSource.Server;
import com.creditease.dbus.heartbeat.type.MaasMessage.SchemaInfo.Column;
import com.creditease.dbus.heartbeat.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by dashencui on 2017/9/14.
 */
public class DbusDataDaoImpl implements IDbusDataDao {

    public boolean testQuery(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean is_seccess = true;
        int test = 2;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);

            String sql = "SELECT 1";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                test = rs.getInt(1);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        if (test == 1)
            is_seccess = true;
        return is_seccess;

    }

    public List<Long> queryDsFromDbusData(String key, String host, String port, String instance_name, String schmaName, String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int count = 0;
        List<Long> dsList = new ArrayList<Long>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String a_like = "%" + host + "%" + port + "%" + instance_name + "%";

            if (tableName.isEmpty()) {
                String sql = "SELECT  ver_id FROM t_data_tables WHERE ds_id IN" +
                        "(SELECT t_dbus_datasource.id FROM t_dbus_datasource WHERE " +
                        "t_dbus_datasource.master_url LIKE ? OR t_dbus_datasource.slave_url LIKE ?)" +
                        "AND SCHEMA_NAME = ?";
                ps = conn.prepareStatement(sql);
                ps.setString(1, a_like);
                ps.setString(2, a_like);
                ps.setString(3, schmaName);
                rs = ps.executeQuery();
            } else {
                String sql = "SELECT  ver_id FROM t_data_tables WHERE ds_id IN" +
                        "(SELECT t_dbus_datasource.id FROM t_dbus_datasource WHERE t_dbus_datasource.master_url LIKE ? " +
                        "OR t_dbus_datasource.slave_url LIKE ?)" +
                        "AND SCHEMA_NAME = ? AND TABLE_NAME = ? ";
                ps = conn.prepareStatement(sql);
                ps.setString(1, a_like);
                ps.setString(2, a_like);
                ps.setString(3, schmaName);
                ps.setString(4, tableName);
                rs = ps.executeQuery();
            }
            while (rs.next()) {
                LoggerFactory.getLogger().info("[count]" + dsList.size());
                long ds_id = rs.getLong("ver_id");
                dsList.add(ds_id);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return dsList;
    }

    //获取对应的数据源个数
    public int queryCountDsId(String key, String host, String port, String instance_name) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int count = 0;
        int count_ds_id = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String a_like = "%" + host + "%" + port + "%" + instance_name + "%";

            String sql = "SELECT COUNT(id) FROM t_dbus_datasource WHERE t_dbus_datasource.master_url LIKE ? " +
                    "OR t_dbus_datasource.master_url LIKE ?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, a_like);
            ps.setString(2, a_like);
            rs = ps.executeQuery();
            while (rs.next()) {
                count_ds_id = rs.getInt(1);
                LoggerFactory.getLogger().info("[type of select count]", rs.getClass());
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return count_ds_id;
    }

    //获取一个数据源，当根据非ds_name信息能获取到一个数据源时，获取；否则，根据ds_name获取数据源信息
    public List<Long> queryDsFromDbusData_use_dsname(String key, String ds_name, String schema_name, String table_name) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        List<Long> ver_id_list = new ArrayList<>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            if (table_name.isEmpty()) {
                String sql = "SELECT  t_data_tables.ver_id FROM t_data_tables,t_dbus_datasource " +
                        "WHERE t_dbus_datasource.ds_name = ?" +
                        "AND t_data_tables.schema_name = ? " +
                        "AND t_dbus_datasource.id = t_data_tables.ds_id";

                ps = conn.prepareStatement(sql);
                ps.setString(1, ds_name);
                ps.setString(2, schema_name);
                rs = ps.executeQuery();
            } else {
                String sql = "SELECT  t_data_tables.ver_id FROM t_data_tables,t_dbus_datasource " +
                        "WHERE t_dbus_datasource.ds_name = ? " +
                        "AND t_data_tables.schema_name = ? " +
                        "AND t_data_tables.table_name = ? " +
                        "AND t_dbus_datasource.id = t_data_tables.ds_id";

                ps = conn.prepareStatement(sql);
                ps.setString(1, ds_name);
                ps.setString(2, schema_name);
                ps.setString(3, table_name);
                rs = ps.executeQuery();
            }
            while (rs.next()) {
                long ver_id = rs.getLong("ver_id");
                ver_id_list.add(ver_id);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }

        return ver_id_list;

    }

    public String queryDsType(String key, String dsName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String dsType = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT t_dbus_datasource.ds_type FROM t_dbus_datasource WHERE " +
                    "t_dbus_datasource.ds_name = ? ";
            ps = conn.prepareStatement(sql);
            ps.setString(1, dsName);
            rs = ps.executeQuery();
            while (rs.next()) {
                dsType = rs.getString("ds_type");
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return dsType;
    }

    public String queryTableComment(String key, long ver_id, String schemaName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String tableComment = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT t_meta_version.comments FROM t_meta_version,t_data_tables WHERE  " +
                    "t_data_tables.ver_id = ? AND t_data_tables.ver_id = t_meta_version.id";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ver_id);
            rs = ps.executeQuery();
            while (rs.next()) {
                tableComment = rs.getString("comments");
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return tableComment;
    }

    public String queryTableComment2(String key, long ds_id, String schemaName, String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String tableComment = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT t_meta_version.comments FROM t_meta_version WHERE  " +
                    "ds_id = ? AND schema_name = ? AND table_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ds_id);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            rs = ps.executeQuery();
            while (rs.next()) {
                tableComment = rs.getString("comments");
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return tableComment;
    }

    public String queryTableName(String key, long ver_id, String schemaName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String tableName = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT table_name FROM t_data_tables " +
                    "WHERE ver_id = ? AND schema_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ver_id);
            ps.setString(2, schemaName);
            rs = ps.executeQuery();
            while (rs.next()) {
                tableName = rs.getString("table_name");
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return tableName;
    }


    //只根据ver_id查询所有列信息
    public List<Column> queryColumsUseVerid(String key, long ver_id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Column> column_list = new ArrayList<>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT * FROM t_table_meta WHERE ver_id = ?";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ver_id);
            rs = ps.executeQuery();
            while (rs.next()) {
                Column column = new Column();
                column.setColumn_name(rs.getString("original_column_name"));
                column.setColumn_type(rs.getString("data_type"));
                column.setNullable(rs.getString("nullable"));
                column.setColumn_comment(rs.getString("comments"));
                column.setData_length(rs.getString("data_length"));
                column.setData_precision(rs.getString("data_precision"));
                column.setData_scale(rs.getString("data_scale"));
                column.setIs_pk(rs.getString("is_pk"));
                column.setPk_position(rs.getString("pk_position"));
                column_list.add(column);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return column_list;
    }

    //根据ds_id，schemaName，tableName查询所有列信息，用于增量消息的查询
    public List<Column> queryColumsUseMore(String key, long ds_id, String schemaName, String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Column> column_list = new ArrayList<Column>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT * FROM t_table_meta WHERE ver_id IN " +
                    "(SELECT  t_data_tables.ver_id FROM t_dbus_datasource,t_data_tables " +
                    "  WHERE " +
                    " t_data_tables.ds_id  = ?" +
                    " AND t_data_tables.schema_name = ?" +
                    " AND t_data_tables.table_name = ?)";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ds_id);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            rs = ps.executeQuery();
            while (rs.next()) {
                Column temp_column = new Column();
                temp_column.setColumn_name(rs.getString("original_column_name"));
                temp_column.setColumn_type(rs.getString("data_type"));
                temp_column.setNullable(rs.getString("nullable"));
                temp_column.setColumn_comment(rs.getString("comments"));
                temp_column.setData_length(rs.getString("data_length"));
                temp_column.setData_precision(rs.getString("data_precision"));
                temp_column.setData_scale(rs.getString("data_scale"));
                temp_column.setIs_pk(rs.getString("is_pk"));
                temp_column.setPk_position(rs.getString("pk_position"));
                column_list.add(temp_column);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return column_list;
    }

    public Column queryColumForAppender(String key, long ds_id, String schemaName, String tableName, String columnName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Column column = new Column();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT * FROM t_table_meta WHERE ver_id IN " +
                    "(SELECT  t_data_tables.ver_id FROM t_dbus_datasource,t_data_tables " +
                    "  WHERE " +
                    " t_data_tables.ds_id  = ?" +
                    " AND t_data_tables.schema_name = ?" +
                    " AND t_data_tables.table_name = ?)" +
                    " AND original_column_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ds_id);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            ps.setString(4, columnName);
            rs = ps.executeQuery();
            while (rs.next()) {
                column.setColumn_name(rs.getString("original_column_name"));
                column.setColumn_type(rs.getString("data_type"));
                column.setNullable(rs.getString("nullable"));
                column.setColumn_comment(rs.getString("comments"));
                column.setData_length(rs.getString("data_length"));
                column.setData_precision(rs.getString("data_precision"));
                column.setData_scale(rs.getString("data_scale"));
                column.setIs_pk(rs.getString("is_pk"));
                column.setPk_position(rs.getString("pk_position"));
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return column;
    }

    public DataSource queryDsInfoFromDbusData(String key, long ver_id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        MaasMessage.DataSource dataSource = new MaasMessage.DataSource();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT t_dbus_datasource.* FROM t_dbus_datasource,t_data_tables WHERE t_data_tables.ver_id = ?" +
                    " AND t_data_tables.ds_id = t_dbus_datasource.id ";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ver_id);
            rs = ps.executeQuery();
            while (rs.next()) {
                String ds_type = rs.getString("ds_type");
                dataSource.setDatabase_type(ds_type);
                String master_url = rs.getString("master_url");
                LoggerFactory.getLogger().info("[master_url]" + master_url);
                String slave_url = rs.getString("slave_url");
                List<Server> server_list = new ArrayList<>();
                if (ds_type.equals("mysql")) {

                    String[] host_port = getserverFromMysql(master_url);
                    Server server = new Server();
                    server.setHost(host_port[0]);
                    server.setPort(host_port[1]);
                    server_list.add(server);

                    host_port = getserverFromMysql(slave_url);
                    Server server1 = new Server();
                    server1.setHost(host_port[0]);
                    server1.setPort(host_port[1]);
                    if (!server_list.contains(server1)) {
                        server_list.add(server1);
                    }
                    dataSource.setInstance_name("mysql[]");
                } else {
                    String instance_name = getInstancenameFronOracle(master_url);
                    dataSource.setInstance_name(instance_name);
                    String url = master_url + slave_url;
                    server_list = getserverFromOracle(url);
                }
                dataSource.setServer(server_list);

            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return dataSource;
    }

    public DataSource queryDsInfoUseDsId(String key, long ds_id) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        MaasMessage.DataSource dataSource = new MaasMessage.DataSource();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            String sql = "SELECT t_dbus_datasource.* FROM t_dbus_datasource WHERE id = ? ";
            ps = conn.prepareStatement(sql);
            ps.setLong(1, ds_id);
            rs = ps.executeQuery();
            while (rs.next()) {
                String ds_type = rs.getString("ds_type");
                dataSource.setDatabase_type(ds_type);
                String master_url = rs.getString("master_url");
                String slave_url = rs.getString("slave_url");
                List<Server> server_list = new ArrayList<>();
                if (ds_type.equals("mysql")) {
                    dataSource.setInstance_name("mysql[]");

                    String[] host_port = getserverFromMysql(master_url);
                    Server server = new Server();
                    server.setHost(host_port[0]);
                    server.setPort(host_port[1]);
                    server_list.add(server);

                    host_port = getserverFromMysql(slave_url);
                    Server server1 = new Server();
                    server1.setHost(host_port[0]);
                    server1.setPort(host_port[1]);
                    if (!server_list.contains(server)) {
                        server_list.add(server1);
                    }

                } else {
                    String instance_name = getInstancenameFronOracle(master_url);
                    dataSource.setInstance_name(instance_name);
                    String url = master_url + slave_url;
                    server_list = getserverFromOracle(url);
                }
                dataSource.setServer(server_list);

            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-mgrDataDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return dataSource;
    }

    public String[] getserverFromMysql(String url) {
        int index1 = url.indexOf("//");
        int index2 = url.indexOf("/", index1 + 3);
        String hostport = url.substring(index1 + 2, index2);
        LoggerFactory.getLogger().info("[hostport]" + hostport);
        String[] host_port = hostport.split(":");
        return host_port;
    }

    public String getInstancenameFronOracle(String url) {
        String instance_name;
        int indexOfServercename = url.indexOf("SERVICE_NAME");
        int indexOfSid = url.indexOf("SID");
        if (!(indexOfServercename == -1)) {
            int index1 = url.indexOf("=", indexOfServercename);
            int index2 = url.indexOf(")", indexOfServercename);
            instance_name = url.substring(index1 + 1, index2);
            instance_name = instance_name.trim();
        } else {
            int index1 = url.indexOf("=", indexOfSid);
            int index2 = url.indexOf(")", indexOfSid);
            instance_name = url.substring(index1 + 1, index2);
            instance_name = instance_name.trim();
        }
        return instance_name;
    }

    public List<Server> getserverFromOracle(String url) {
        LoggerFactory.getLogger().info("url" + url);
        int indexOfHost = 0;
        int indexOfPort = 0;
        List<Server> servers = new ArrayList<>();

        while (indexOfHost != -1 && indexOfPort != -1 && indexOfPort + 90 < url.length()) {
            Server server = new Server();
            indexOfHost = url.indexOf("HOST", indexOfHost + 4);
            indexOfPort = url.indexOf("PORT", indexOfPort + 4);
            int index1 = url.indexOf("=", indexOfHost);
            int index2 = url.indexOf(")", indexOfHost);
            server.setHost(url.substring(index1 + 1, index2).trim());
            int index3 = url.indexOf("=", indexOfPort);
            int index4 = url.indexOf(")", indexOfPort);
            server.setPort(url.substring(index3 + 1, index4).trim());
            if (!servers.contains(server)) {
                servers.add(server);
            }
        }
        return servers;
    }

    public List<Server> removeServerListDupli(List<Server> stringList) {
        Set<Server> set = new LinkedHashSet<>();
        set.addAll(stringList);

        stringList.clear();

        stringList.addAll(set);
        return stringList;
    }
}
