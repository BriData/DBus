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


package com.creditease.dbus.dbaccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Shrimp on 16/5/17.
 * 拉全量访问数据库时，写的数据库访问公共类。
 * 后来拉全量模块统一使用sqoop借鉴过来的一套，便弃用这个类了。将来如果没人用这个，可以删除。需要用的话，继续完善补充。
 */
public class DbAccessHelper implements IDbAccessHelper {
    private Logger logger = LoggerFactory.getLogger(DbAccessHelper.class);
    private DataSource ds;
    private Connection conn = null;
    private PreparedStatement ps = null;

    public DbAccessHelper(Properties properties) throws Exception {
        DataSourceProvider provider = new DruidDataSourceProvider(properties);
        this.ds = provider.provideDataSource();
    }

    @Override
    public ResultSet executeSql(String sql) throws Exception {
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            return resultSet;
        } catch (SQLException se) {
            logger.error("Fetch Meta information error!", se);
            if (ps != null) ps.close();
            if (conn != null) conn.close();
            throw se;
        }
    }

    @Override
    public void close() {
        if (ps != null)
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error("PreparedStatement closing failed!");
            }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("DB connection closing failed!");
            }
    }
}
