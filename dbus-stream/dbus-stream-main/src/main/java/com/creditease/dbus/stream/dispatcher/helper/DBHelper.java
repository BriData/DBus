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

    public DBHelper (String zkServers) {
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
                dsInfo.setDbSourceType(rs.getString("ds_type"));
                dsInfo.setDataTopic(rs.getString("topic"));
                dsInfo.setCtrlTopic(rs.getString("ctrl_topic"));
                dsInfo.setDbusSchema(rs.getString("dbus_user"));

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



    public void close() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        }catch (Exception ex) {
            logger.error("Close Mysql connection error:" + ex.getMessage());
        }
    }
}
