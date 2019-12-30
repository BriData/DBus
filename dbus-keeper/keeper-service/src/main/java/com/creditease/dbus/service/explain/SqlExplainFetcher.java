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


package com.creditease.dbus.service.explain;

import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.enums.DbusDatasourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/28
 */
public abstract class SqlExplainFetcher {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private DataSource ds;
    private Connection conn;

    public SqlExplainFetcher(DataSource ds) {
        this.ds = ds;
    }

    public abstract String buildQuery(DataTable dataTable, String codition);

    public boolean sqlExplain(DataTable dataTable, String codition) throws Exception {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String sql = null;
        try {
            sql = buildQuery(dataTable, codition);
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            statement.execute();
            return true;
        } catch (Exception e) {
            logger.warn("Error codition :{}", sql);
            logger.error(e.getMessage(), e);
            return false;
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    public static SqlExplainFetcher getFetcher(DataSource ds) throws Exception {
        SqlExplainFetcher fetcher;
        DbusDatasourceType dsType = DbusDatasourceType.parse(ds.getDsType());
        switch (dsType) {
            case MYSQL:
                Class.forName("com.mysql.jdbc.Driver");
                fetcher = new MySqlExplainFetcher(ds);
                break;
            case ORACLE:
                Class.forName("oracle.jdbc.driver.OracleDriver");
                fetcher = new OracleExplainFetcher(ds);
                break;
            //case DB2:
            //    Class.forName("com.ibm.db2.jcc.DB2Driver");
            //    break;
            default:
                throw new IllegalArgumentException();
        }
        Connection conn = DriverManager.getConnection(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
        fetcher.setConnection(conn);
        return fetcher;
    }

    protected void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void close() throws Exception {
        if (this.conn != null) {
            conn.close();
        }
    }

}
