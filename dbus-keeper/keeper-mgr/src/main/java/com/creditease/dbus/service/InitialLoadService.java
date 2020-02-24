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


package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 发送oracle/mysql拉全量请求
 */
@Service
public class InitialLoadService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public String getMysqlTables(Connection conn, DataTable tab) throws Exception {
        String sql = "select table_name from information_schema.tables t where t.table_schema = ?";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, tab.getSchemaName());
            rs = statement.executeQuery();
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
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    public String getOracleTables(Connection conn, DataTable table) throws Exception {
        String sql = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER= ?";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, table.getSchemaName());
            rs = statement.executeQuery();
            StringBuilder buf = new StringBuilder();
            String name = "";
            Pattern p = Pattern.compile(table.getPhysicalTableRegex());
            logger.info("正则表达式为：{}", table.getPhysicalTableRegex());
            while (rs.next()) {
                name = rs.getString("TABLE_NAME");
                //logger.info("查询到表名：{}", name);
                Matcher matcher = p.matcher(name);
                if (matcher.matches()) {
                    logger.info("匹配到表名：{}", name);
                    buf.append(name).append(";");
                }
            }
            if (buf.length() > 0) {
                buf.deleteCharAt(buf.length() - 1);
            }
            return buf.toString();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

}
