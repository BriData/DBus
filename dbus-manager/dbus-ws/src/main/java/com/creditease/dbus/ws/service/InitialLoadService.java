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

package com.creditease.dbus.ws.service;

import com.creditease.dbus.ws.domain.DataTable;
import com.creditease.dbus.ws.domain.DbusDataSource;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 发送oracle/mysql拉全量请求
 * Created by Shrimp on 16/9/8.
 */
public class InitialLoadService {
    private static final String PROCEDURE_NAME = "create_full_data_req";

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static InitialLoadService getService() {
        return new InitialLoadService();
    }

    public void oracleInitialLoad(long tableId) throws Exception {
        TablesService ts = TablesService.getService();
        DataTable table = ts.getTableById(tableId);
        DataSourceService dataSourceService = DataSourceService.getService();
        DbusDataSource ds = dataSourceService.getDataSourceById(table.getDsID());
        oracleInitialLoad(ds, table);
    }

    public void mysqlInitialLoad(long tableId) throws Exception {
        TablesService ts = TablesService.getService();
        DataTable table = ts.getTableById(tableId);

        DataSourceService dataSourceService = DataSourceService.getService();
        DbusDataSource ds = dataSourceService.getDataSourceById(table.getDsID());
        mysqlInitialLoad(ds, table);
    }

    public void oracleInitialLoad(DbusDataSource ds, DataTable table) throws Exception {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = DriverManager.getConnection(ds.getMasterURL(), ds.getDbusUser(), ds.getDbusPassword());
        CallableStatement statement = null;
        try {
            String procedure = String.format("{call %s.%s(?,?,?)}", ds.getDbusUser(), PROCEDURE_NAME);
            statement = conn.prepareCall(procedure);
            statement.setString(1, table.getSchemaName());
            statement.setString(2, table.getTableName());
            statement.setString(3, ds.getDsName());

            statement.execute();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }

    public void mysqlInitialLoad(DbusDataSource ds, DataTable table) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(ds.getMasterURL(), ds.getDbusUser(), ds.getDbusPassword());
        CallableStatement statement = null;
        try {
            String procedure = String.format("{call %s.%s(?,?,?,?)}", ds.getDbusUser(), PROCEDURE_NAME);

            statement = conn.prepareCall(procedure);
            statement.setString(1, table.getSchemaName());
            statement.setString(2, table.getTableName());
            String nameString = Strings.isNullOrEmpty(table.getPhysicalTableRegex()) ? null : getMysqlTables(conn, table);
            statement.setString(3, nameString);
            statement.setString(4, ds.getDsName());

            statement.executeUpdate();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }


    public void oracleInitialLoadBySql(DbusDataSource ds, DataTable table, long seqno) throws Exception {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = DriverManager.getConnection(ds.getMasterURL(), ds.getDbusUser(), ds.getDbusPassword());
        PreparedStatement pst = null;
        try {
            String sql = "insert into db_full_pull_requests(" +
                    "seqno," +
                    "schema_name," +
                    "table_name," +
                    "scn_no," +
                    "split_col," +
                    "split_bounding_query," +
                    "pull_target_cols," +
                    "pull_req_create_time," +
                    "pull_start_time," +
                    "pull_end_time," +
                    "pull_status," +
                    "pull_remark)" +
                    "  values(" +
                    "?," +
                    "upper(?)," +
                    "upper(?)," +
                    "dbms_flashback.get_system_change_number," +
                    "null," +
                    "null," +
                    "null," +
                    "systimestamp," +
                    "null," +
                    "null," +
                    "null," +
                    "?" +
                    ")";

            pst = conn.prepareStatement(sql);
            pst.setLong(1, seqno);
            pst.setString(2, table.getSchemaName());
            pst.setString(3, table.getTableName());
            pst.setString(4, ds.getDsName());

            pst.executeUpdate();
            logger.info("Insert into source table db_full_pull_requests ok, masterUrl:{}, ds:{}, schema:{}, table:{}",ds.getMasterURL(), ds.getDsName(), table.getSchemaName(), table.getTableName());
        } catch (Exception e) {
            logger.error("Error insert into oracle source table db_full_pull_requests ds:{}, schema:{}, table:{}, exception:{} ", ds.getDsName(), table.getSchemaName(), table.getTableName(), e);
        }
        finally {
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }

    public void mysqlInitialLoadBySql(DbusDataSource ds, DataTable table, long seqno) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(ds.getMasterURL(), ds.getDbusUser(), ds.getDbusPassword());
        PreparedStatement pst = null;
        try {
            String sql = "insert into db_full_pull_requests (" +
                    "seqno," +
                    "schema_name," +
                    "table_name," +
                    "physical_tables," +
                    "scn_no," +
                    "split_col," +
                    "split_bounding_query," +
                    "pull_target_cols," +
                    "pull_req_create_time," +
                    "pull_start_time," +
                    "pull_end_time," +
                    "pull_status," +
                    "pull_remark)" +
                    "values(" +
                    "?," +
                    "?," +
                    "?," +
                    "?," +
                    "null," +
                    "null," +
                    "null," +
                    "null," +
                    "CURRENT_TIMESTAMP()," +
                    "null," +
                    "null," +
                    "null," +
                    "?" +
                    ");";
            pst = conn.prepareStatement(sql);
            pst.setLong(1, seqno);
            pst.setString(2, table.getSchemaName());
            pst.setString(3, table.getTableName());
            String nameString = Strings.isNullOrEmpty(table.getPhysicalTableRegex()) ? null : getMysqlTables(conn, table);
            pst.setString(4, nameString);
            pst.setString(5, ds.getDsName());

            pst.executeUpdate();
            logger.info("Insert into source table db_full_pull_requests ok, masterUrl:{}, ds:{}, schema:{}, table:{}",ds.getMasterURL(), ds.getDsName(), table.getSchemaName(), table.getTableName());
        } catch(Exception e) {
            logger.error("Error insert into oracle source table db_full_pull_requests ds:{}, schema:{}, table:{}, exception:{} ", ds.getDsName(), table.getSchemaName(), table.getTableName(), e);
        }
        finally {
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private String getMysqlTables(Connection conn, DataTable tab) throws Exception {
        String sql = "select table_name from information_schema.tables t where t.table_schema = ?";
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, tab.getSchemaName());
            ResultSet rs = statement.executeQuery();
            StringBuilder buf = new StringBuilder();
            String name = "";
            Pattern p = Pattern.compile(tab.getPhysicalTableRegex());
            while (rs.next()) {
                name = rs.getString("table_name");
                Matcher matcher = p.matcher(name);
                if (matcher.matches()) {
                    buf.append(name).append(";");
                }
            }
            if (buf.length() > 0) {
                buf.deleteCharAt(buf.length() - 1);
            }
            return buf.toString();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }
}
