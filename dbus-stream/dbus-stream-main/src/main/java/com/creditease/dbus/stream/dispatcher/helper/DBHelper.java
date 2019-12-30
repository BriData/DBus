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


package com.creditease.dbus.stream.dispatcher.helper;

import com.creditease.dbus.commons.ConnectionProvider;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by dongwang47 on 2016/8/17.
 */
public class DBHelper {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String zkServers;
    private Connection connection = null;

    public DBHelper(String zkServers) {
        this.zkServers = zkServers;
    }

    public Connection getMysqlConnection() throws Exception {
        if (connection == null) {
            connection = ConnectionProvider.getInstance().createMySQLConnection(zkServers);
        }
        return connection;
    }

    /**
     * read datasource_name, datatopic, ctrlTopic, dbusSchemaName from Mysql
     *
     * @throws Exception
     */
    public void loadDsInfo(DataSourceInfo dsInfo) throws Exception {

        PreparedStatement stmt = null;

        try {
            connection = getMysqlConnection();

            String sqlQuery = "select ds_type, topic, ctrl_topic, dbus_user " +
                    "from t_dbus_datasource where ds_name = ?";

            stmt = connection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            stmt.setString(1, dsInfo.getDbSourceName());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                // One row of data in the ResultSet object
                String type = rs.getString("ds_type");
                dsInfo.setDbSourceType(rs.getString("ds_type"));
                dsInfo.setDataTopic(rs.getString("topic"));
                dsInfo.setCtrlTopic(rs.getString("ctrl_topic"));

                // dbus 默认管理库 为 dbus，mysql小写，oracle 大写
                dsInfo.setDbusSchema("dbus");
                if (type != null) {
                    if (type.equalsIgnoreCase("oracle")) {
                        dsInfo.setDbusSchema("DBUS");
                    } else if (type.equalsIgnoreCase("mysql")) {
                        dsInfo.setDbusSchema("dbus");
                    } else {
                        ;
                    }
                }

                // Set data topic and control topic
                if (Strings.isNullOrEmpty(dsInfo.getDbSourceType())
                        || Strings.isNullOrEmpty(dsInfo.getDataTopic())
                        || Strings.isNullOrEmpty(dsInfo.getCtrlTopic())
                        || Strings.isNullOrEmpty(dsInfo.getDbusSchema())) {
                    logger.error("Read from mysql error. Data topic or control topic or dbus schema is empty! Exit.");
                    throw new RuntimeException("Read mysql DB error!, Data topic or control topic or dbus schema is empty!");
                }
            }

        } catch (Exception e) {
            logger.error("Exception caught when setting up properties!");
            e.printStackTrace();
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * getSchemaProps
     *
     * @param dsInfo
     * @return Properties schemaTopicProps
     * ref: http://www.mkyong.com/jdbc/how-to-connect-to-mysql-with-jdbc-driver-java/
     * https://docs.oracle.com/javase/tutorial/jdbc/basics/processingsqlstatements.html
     */
    public Properties getSchemaProps(DataSourceInfo dsInfo) throws Exception {
        PreparedStatement stmt = null;

        // Create and return the Schema Properties object that we will return
        Properties schemaTopicProps = new Properties();

        try {
            connection = getMysqlConnection();

            String sqlQuery = "select s.schema_name as `schema`, s.src_topic as topic " +
                    "from t_data_schema as s, t_dbus_datasource as d " +
                    "where s.ds_id = d.id and d.ds_type=? " +
                    "and d.ds_name = ?;";
            stmt = connection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            stmt.setString(1, dsInfo.getDbSourceType());
            stmt.setString(2, dsInfo.getDbSourceName());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                // One row of data in the ResultSet object
                String schema = rs.getString("schema");
                String topic = rs.getString("topic");

                // Set schema=topic pair
                if (!Strings.isNullOrEmpty(schema) && !Strings.isNullOrEmpty(topic)) {
                    schemaTopicProps.setProperty(schema, topic);
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        return schemaTopicProps;
    }


    public Set<String> loadTablesInfo(DataSourceInfo dsInfo) throws Exception {

        PreparedStatement stmt = null;

        try {
            connection = getMysqlConnection();
            int dsId = Integer.MIN_VALUE;
            Set<String> tables = new HashSet<>();

            String sqlQuery = "select id " +
                    "from t_dbus_datasource where ds_name = ?";

            stmt = connection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            stmt.setString(1, dsInfo.getDbSourceName());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                // One row of data in the ResultSet object
                dsId = rs.getInt(1);
            }

            if (dsId != Integer.MIN_VALUE) {
                sqlQuery = "select SCHEMA_NAME,TABLE_NAME " +
                        "from t_data_tables where ds_id = ? AND status = 'ok'";
                stmt = connection.prepareStatement(sqlQuery);
                stmt.setInt(1, dsId);
                rs = stmt.executeQuery();

                while (rs.next()) {
                    //cdc生成的topic，都是小写的，但db2中的表都是大写的，DBus管理库中的表也是大写的，因此此处将对管理库中的表进行小写转换
                    tables.add(StringUtils.joinWith(".", dsInfo.getDbSourceName(), rs.getString("SCHEMA_NAME").toLowerCase(), rs.getString("TABLE_NAME").toLowerCase()));
                }

                return tables;

            }

        } catch (Exception e) {
            logger.error("Exception caught when setting up properties!");
            e.printStackTrace();
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return null;
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception ex) {
            logger.error("Close Mysql connection error:" + ex.getMessage());
        }
    }
}
