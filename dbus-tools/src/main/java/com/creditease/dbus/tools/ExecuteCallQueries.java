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

package com.creditease.dbus.tools;

import com.creditease.dbus.commons.ConnectionProvider;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.tools.common.ConfUtils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Created by 201605240095 on 2016/7/25.
 */
@Deprecated
public class ExecuteCallQueries {

    private final static String FULL_DATA_REQ = "ExecuteCallQueries/create_full_data_req.properties";
    private final static String FULL_DATA_REQ2 = "ExecuteCallQueries/create_full_data_req2.properties";
    private final static String FORCE_SYNC_META = "ExecuteCallQueries/force_sync_meta.properties";

    private String zkServer;

    private String connStr;
    private String dbusUser;
    private String dbusPasswd;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Properties fullDataReqProps;
    private Properties fullDataReq2Props;
    private Properties forceSyncMetaProps;

    public ExecuteCallQueries () throws IOException {
        // Parse Queries
        fullDataReqProps = ConfUtils.getProps(FULL_DATA_REQ);
        fullDataReq2Props = ConfUtils.getProps(FULL_DATA_REQ2);
        forceSyncMetaProps  = ConfUtils.getProps(FORCE_SYNC_META);
    }

    private Connection getMysqlConn () throws Exception {
        try {
            // Parse zookeeper
            zkServer = ConfUtils.loadZKProperties().getProperty(Constants.ZOOKEEPER_SERVERS);
            // Get a SQL Connection object
            Connection mysqlConnection = ConnectionProvider.getInstance().createMySQLConnection(zkServer);
            return mysqlConnection;
        } catch (Exception e) {
            logger.info("Exception was caught when setting up mysql connection.");
            e.printStackTrace();
            throw e;
        }
    }

    private Connection createOracleDBConnection () throws Exception {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            logger.error("Oracle JDBC Driver not found! ");
            e.printStackTrace();
            throw e;
        }
        try {
            Connection connection = DriverManager.getConnection(
                    connStr, dbusUser,
                    dbusPasswd);
            return connection;
        } catch (SQLException e) {
            logger.error("Connection to Oracle DB failed!");
            e.printStackTrace();
            throw e;
        }
    }

    private void setLoginInfo (Connection mysqlConnection, String ds_name) throws Exception {
        String sqlQuery = "select master_url, dbus_user, dbus_pwd " +
                "from t_dbus_datasource as d " +
                "where d.ds_name = ?";

        // Set conn_string, user, passed, ds_id
        PreparedStatement stmt = null;
        try {
            stmt = mysqlConnection.prepareStatement(sqlQuery);
            // Fill out datasource name for sql query
            // TODO: May needs to be changed
            stmt.setString(1, ds_name);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                connStr = rs.getString("master_url");
                dbusUser = rs.getString("dbus_user");
                dbusPasswd = rs.getString("dbus_pwd");
            }

        } catch (SQLException e) {
            logger.error("Exception caught when fetching login info from MySQL db. ");
            e.printStackTrace();
            throw e;
        } finally {
            closeStmt(stmt);
            logger.info("Successfully fetched master_url, dbus_user, and dbus_password used for accessing Oracle DB. ");
        }
    }

    private void closeStmt (Statement stmt) throws Exception {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("Exception caught when trying to close a statement. ");
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void closeConn (Connection conn) throws Exception {
        try {
            if (conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("Exception caught when close a connection. ");
            e.printStackTrace();
            throw e;
        }
    }

    public void executeFullDataReq () throws Exception {
        Connection mysqlConn = null;
        Connection oracleConnection = null;
        PreparedStatement stmt = null;
        String p_schema = fullDataReqProps.getProperty("p_schema");
        String p_table_name = fullDataReqProps.getProperty("p_table_name");
        String ds_name=fullDataReqProps.getProperty("ds_name");
        if(StringUtils.isNotBlank(p_table_name)){
            try {

                mysqlConn = getMysqlConn();
                // Connect to MySQL db, and set log in info for Oracle DB
                setLoginInfo(mysqlConn, ds_name);

                // Connect to Oracle DB
                oracleConnection = createOracleDBConnection();

                String[] tables=p_table_name.split(",");
                for (String table : tables) {
                    // Set query
                    // TODO: Should DBUS also be given?
                    String fullDataQuery = "call DBUS.create_full_data_req(?,?)";
                    stmt = oracleConnection.prepareStatement(fullDataQuery);
                    stmt.setString(1, p_schema);
                    stmt.setString(2, table);
                    stmt.execute();
                    logger.info("Send full pull request for: Schema-{},table-{}.",p_schema,table);
                }
            } catch (Exception e) {
                logger.error("Exception was caught when executing create_full_data_req!");
                e.printStackTrace();
                throw e;
            } finally {
                closeConn(mysqlConn);
                closeConn(oracleConnection);
                closeStmt(stmt);
                logger.info("Executing create_full_data_req finished.");
            }
        }
    }

    public void executeFullDataReq2 () throws Exception {
        Connection mysqlConn = null;
        Connection oracleConnection = null;
        PreparedStatement stmt = null;
        String p_schema = fullDataReq2Props.getProperty("p_schema");
        String p_table_name = fullDataReq2Props.getProperty("p_table_name");
        int p_scn = Integer.parseInt(fullDataReq2Props.getProperty("p_scn"));
        String p_split_col = fullDataReq2Props.getProperty("p_split_col");
        String p_target_col = fullDataReq2Props.getProperty("p_target_col");
        String ds_name = fullDataReq2Props.getProperty("ds_name");

        try {

            mysqlConn = getMysqlConn();
            // Connect to MySQL db, and set log in info for Oracle DB
            setLoginInfo(mysqlConn, ds_name);

            // Connect to Oracle DB
            oracleConnection = createOracleDBConnection();

            // Set query
            // TODO: Should DBUS also be given?
            String fullData2Query = "call dbus.create_full_data_req2(?, ?, ?, ?, ?)";
            stmt = oracleConnection.prepareStatement(fullData2Query);
            stmt.setString(1, p_schema);
            stmt.setString(2, p_table_name);
            stmt.setInt(3, p_scn);
            stmt.setString(4, p_split_col);
            stmt.setString(5, p_target_col);

            stmt.execute();

        } catch (Exception e) {
            logger.error("Exception was caught when executing create_full_data_req2!");
            e.printStackTrace();
            throw e;
        } finally {
            closeConn(mysqlConn);
            closeConn(oracleConnection);
            closeStmt(stmt);
            logger.info("Executing create_full_data_req2 finished.");
        }
    }


    public void executeForceSyncMeta () throws Exception {
        Connection mysqlConn = null;
        Connection oracleConnection = null;
        PreparedStatement stmt = null;
        String p_schema = forceSyncMetaProps.getProperty("p_schema");
        String p_table = forceSyncMetaProps.getProperty("p_table");
        String ds_name = forceSyncMetaProps.getProperty("ds_name");

        try {

            mysqlConn = getMysqlConn();
            // Connect to MySQL db, and set log in info for Oracle DB
            setLoginInfo(mysqlConn, ds_name);

            // Connect to Oracle DB
            oracleConnection = createOracleDBConnection();

            // Set query
            // TODO: Should DBUS also be given?
            String forceSyncMetaQuery = "call DBUS.force_sync_meta(?, ?)";
            stmt = oracleConnection.prepareStatement(forceSyncMetaQuery);
            stmt.setString(1, p_schema);
            stmt.setString(2, p_table);

            stmt.execute();

        } catch (Exception e) {
            logger.error("Exception was caught when executing force_sync_meta!");
            e.printStackTrace();
            throw e;
        } finally {
            closeConn(mysqlConn);
            closeConn(oracleConnection);
            closeStmt(stmt);
            logger.info("Executing force_sync_meta finished.");
        }
    }


    public static void main(String[] args) throws Exception {

        ExecuteCallQueries ecq = new ExecuteCallQueries();

        if (args.length != 1) {
            System.out.println("Please provide a legal option. Example: create_full_data_req, create_full_data_req2, " +
                    "or force_sync_meta.");
            return;
        }

        String option = args[0];
        if (option.equals("create_full_data_req")) {
            ecq.executeFullDataReq();
        } else if (option.equals("create_full_data_req2")) {
            ecq.executeFullDataReq2();
        } else if (option.equals("force_sync_meta")){
            ecq.executeForceSyncMeta();
        } else {
            System.out.println("Please provide a legal option. Example: create_full_data_req, create_full_data_req2, " +
                    "or force_sync_meta.");
        }
    }

}
