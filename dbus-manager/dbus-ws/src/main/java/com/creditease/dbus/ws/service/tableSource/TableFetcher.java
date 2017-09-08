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

package com.creditease.dbus.ws.service.tableSource;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.ws.domain.DataTable;
import com.creditease.dbus.ws.domain.DbusDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class TableFetcher {
    private DbusDataSource ds;
    private Connection conn;

    public TableFetcher(DbusDataSource ds) {
        this.ds = ds;
    }

    public abstract String buildQuery(Object... args);


    public abstract String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception;

    public int insertTable(Map<String, Object> params) throws Exception {
        try {
            PreparedStatement statement = conn.prepareStatement(buildQuery(params));
            fillParameters(statement, params);
            int i = statement.executeUpdate();
            return i;
        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }

    public List<DataTable> listTable() throws Exception {
        try {
            String sql = "select * from DBUS_TABLES";
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            return buildResult(resultSet);
        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }

    public static TableFetcher getFetcher(DbusDataSource ds) throws Exception {
        TableFetcher fetcher;
        DbusDatasourceType dsType = DbusDatasourceType.parse(ds.getDsType());
        switch (dsType) {
            case ORACLE:
                Class.forName("oracle.jdbc.driver.OracleDriver");
                fetcher = new OracleTableFetcher(ds);
                break;
            default:
                throw new IllegalArgumentException();
        }
        Connection conn = DriverManager.getConnection(ds.getMasterURL(), ds.getDbusUser(), ds.getDbusPassword());
        fetcher.setConnection(conn);
        return fetcher;
    }
    protected void setConnection(Connection conn) {
        this.conn = conn;
    }

    protected List<DataTable> buildResult(ResultSet rs) throws SQLException {
        List<DataTable> list = new ArrayList<>();
        DataTable table;
        while (rs.next()) {
            table = new DataTable();
            table.setSchemaName(rs.getString("OWNER"));
            table.setTableName(rs.getString("TABLE_NAME"));
            list.add(table);
        }
        return list;
    }
}
