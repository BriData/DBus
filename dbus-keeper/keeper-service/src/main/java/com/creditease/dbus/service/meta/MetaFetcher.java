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

package com.creditease.dbus.service.meta;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.TableMeta;

import java.sql.*;
import java.util.*;

/**
 * Created by zhangyf on 16/9/19.
 */
public abstract class MetaFetcher {
    private DataSource ds;
    private Connection conn;

    public MetaFetcher(DataSource ds) {
        this.ds = ds;

    }

    public abstract String buildQuery(Object... args);

    public abstract String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception;

    public abstract List<TableMeta> buildResult(ResultSet rs) throws SQLException ;

    public List<TableMeta> fetchMeta(Map<String, Object> params) throws Exception {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.prepareStatement(buildQuery(params));
            fillParameters(statement, params);
            resultSet = statement.executeQuery();
            return buildResult(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    public static MetaFetcher getFetcher(DataSource ds) throws Exception {
        MetaFetcher fetcher;
        DbusDatasourceType dsType = DbusDatasourceType.parse(ds.getDsType());
        switch (dsType) {
            case MYSQL:
                Class.forName("com.mysql.jdbc.Driver");
                fetcher = new MySqlMataFetcher(ds);
                break;
            case ORACLE:
                Class.forName("oracle.jdbc.driver.OracleDriver");
                fetcher = new OracleMataFetcher(ds);
                break;
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
