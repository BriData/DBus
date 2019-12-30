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


package com.creditease.dbus.service.schema;

import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.enums.DbusDatasourceType;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SchemaFetcher {
    private DataSource ds;
    private Connection conn;

    public SchemaFetcher(DataSource ds) {
        this.ds = ds;
    }

    public abstract String buildQuery();

    public List<DataSchema> fetchSchema() throws Exception {
        try {
            PreparedStatement statement = conn.prepareStatement(buildQuery());
            ResultSet resultSet = statement.executeQuery();
            return buildResult(resultSet);
        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * 为了解决加表提交的schemaName大小写与源端库查询的结果不一致的问题
     * 该方法返回一个map
     * map的key是schemaName.toUpperCase()
     * map的value是schemaName的实际值
     *
     * @return
     * @throws Exception
     */
    public Map<String, String> getSourceSchemaName() throws Exception {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.prepareStatement(buildQuery());
            resultSet = statement.executeQuery();
            return buildSchemaNameResult(resultSet);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static SchemaFetcher getFetcher(DataSource ds) throws Exception {
        SchemaFetcher fetcher;
        DbusDatasourceType dsType = DbusDatasourceType.parse(ds.getDsType());
        switch (dsType) {
            case MYSQL:
                Class.forName("com.mysql.jdbc.Driver");
                fetcher = new MySqlSchemaFetcher(ds);
                break;
            case ORACLE:
                Class.forName("oracle.jdbc.driver.OracleDriver");
                fetcher = new OracleSchemaFetcher(ds);
                break;
            case DB2:
                Class.forName("com.ibm.db2.jcc.DB2Driver");
                fetcher = new DB2SchemaFetcher(ds);
                break;
            default:
                throw new IllegalArgumentException();
        }
        Connection conn = DriverManager.getConnection(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
        fetcher.setConnection(conn);
        return fetcher;
    }

    protected void setConnection(Connection conn) {
        this.conn = conn;
    }

    protected Map<String, String> buildSchemaNameResult(ResultSet rs) throws SQLException {
        HashMap<String, String> map = new HashMap<>();
        String schemaName;
        while (rs.next()) {
            schemaName = rs.getString("SCHEMANAME");
            map.put(schemaName.toUpperCase(), schemaName);
        }
        return map;
    }

    protected List<DataSchema> buildResult(ResultSet rs) throws SQLException {
        List<DataSchema> list = new ArrayList<>();
        DataSchema schema;
        //ResultSetMetaData rsm = rs.getMetaData();
        //int col = rsm.getColumnCount();
        //String colName = "";
        //for(int i = 0;i<col;i++)
        //{
        //    colName = rsm.getColumnName(i + 1);
        //}
        // System.out.println(colName);
        while (rs.next()) {
            schema = new DataSchema();
            //if("USERNAME".equals(colName))
            //{
            //    schema.setSchemaName(rs.getString("USERNAME"));
            //}
            //else //if("TABLE_SCHEMA".equals(colName))
            //{
            //    schema.setSchemaName(rs.getString("TABLE_SCHEMA"));
            //}
            schema.setSchemaName(rs.getString("SCHEMANAME"));
            schema.setStatus("active");
            list.add(schema);
        }
        return list;
    }

}
