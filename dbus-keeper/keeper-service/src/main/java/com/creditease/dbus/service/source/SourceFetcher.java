/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.service.source;

import com.creditease.dbus.enums.DbusDatasourceType;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SourceFetcher {
    private Connection conn;


    public abstract String buildQuery(Object... args);
    public abstract String buildQuery2(Object... args);
    //public abstract String buildQuery3(Object... args);
    public abstract String buildQuery3(String name);
    public abstract String fillParameters(PreparedStatement statement , String name) throws Exception;
    public abstract String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception;

    public int fetchSource(Map<String, Object> params) throws Exception {
        try {
            PreparedStatement statement = conn.prepareStatement(buildQuery(params));
            fillParameters(statement, params);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            if(params.get("dsType").toString().equals("oracle")){
                return resultSet.getInt("COUNT(SYSDATE)");
            }else{
                return  resultSet.getInt("1");
            }


        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }

    public List<String> fetchTableStructure(Map<String, Object> params) throws Exception {
        try {
               if ((params.get("dsType").toString()).equals("oracle")) {
                   PreparedStatement statementTable = conn.prepareStatement(buildQuery2(params));
                   fillParameters(statementTable, params);
                   ResultSet resultSetOracleTable = statementTable.executeQuery();
                   List<String> list = new ArrayList<>();
                   while (resultSetOracleTable.next()) {
                       list.add(resultSetOracleTable.getString("TABLE_NAME") + "/" + resultSetOracleTable.getString("COLUMN_NAME") + "  , " + resultSetOracleTable.getString("DATA_TYPE") + "\n");
                   }
                   PreparedStatement statementProcedure = conn.prepareStatement(buildQuery3(""));
                   ResultSet resultSetOracleProcedure = statementProcedure.executeQuery();
                   while (resultSetOracleProcedure.next()) {
                       list.add(resultSetOracleProcedure.getString("OBJECT_NAME") + "/" + "--------");
                   }
                   return list;
               } else {
                   PreparedStatement statementName = conn.prepareStatement(buildQuery2(params));
                   fillParameters(statementName, params);
                   ResultSet resultSetMySqlName = statementName.executeQuery();
                   List<String> list = new ArrayList<>();
                   while (resultSetMySqlName.next()) {
                       PreparedStatement statementColumn = conn.prepareStatement(buildQuery3(resultSetMySqlName.getString("Tables_in_dbus")));
                       //fillParameters(statementColumn, resultSetMySqlName.getString("Tables_in_dbus"));
                       ResultSet resultSetMySqlColumn = statementColumn.executeQuery();
                       while (resultSetMySqlColumn.next()) {
                           //list.add(resultSetMySqlName.getString("Tables_in_dbus")+"/"+resultSetMySqlColumn.getString("COLUMN_NAME")+"    "+resultSetMySqlColumn.getString("DATA_TYPE"));
                           list.add(resultSetMySqlName.getString("Tables_in_dbus") + "/" + resultSetMySqlColumn.getString("Field") + "  ,  " + resultSetMySqlColumn.getString("Type") + "\n");
                       }
                       //list.add(resultSetMySqlName.getString("Tables_in_dbus"));
                   }
                   return list;
               }


        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }

    public static SourceFetcher getFetcher(Map<String, Object> params) throws Exception {
        SourceFetcher fetcher;
        switch (DbusDatasourceType.parse(params.get("dsType").toString())) {
            case MYSQL:
                Class.forName("com.mysql.jdbc.Driver");
                fetcher = new MySqlSourceFetcher();
                break;
            case ORACLE:
                Class.forName("oracle.jdbc.driver.OracleDriver");
                fetcher = new OracleSourceFetcher();
                break;
            default:
                throw new IllegalArgumentException();
        }
        Connection conn = DriverManager.getConnection(params.get("URL").toString(),params.get("user").toString() , params.get("password").toString());
        fetcher.setConnection(conn);
        return fetcher;
    }

    protected void setConnection(Connection conn) {
        this.conn = conn;
    }

}
