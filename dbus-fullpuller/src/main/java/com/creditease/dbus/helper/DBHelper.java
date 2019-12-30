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


package com.creditease.dbus.helper;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.*;
import com.creditease.dbus.notopen.db2.DB2Manager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * dbusmgr数据库相关查询
 */
public class DBHelper {
    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    private static Properties mysqlProps = null;
    private static final String DATASOURCE_CONFIG_NAME = FullPullConstants.COMMON_ROOT + "/" + FullPullConstants.MYSQL_CONFIG;
    private static DataSource dataSource = null;

    private static synchronized void initialize() throws Exception {
        if (mysqlProps == null) {
            String topoType = FullPullHelper.getTopologyType();
            mysqlProps = FullPullPropertiesHolder.getProperties(topoType, DATASOURCE_CONFIG_NAME);
        }
    }

    /**
     * Create a connection to the database; usually used only from within
     * getConnection(), which enforces a singleton guarantee around the
     * Connection object.
     * <p>
     * Oracle-specific driver uses READ_COMMITTED which is the weakest
     * semantics Oracle supports.
     *
     * @throws Exception
     * @throws SQLException
     */
    public static Connection getDBusMgrConnection() throws Exception {
        Connection connection;
        if (dataSource == null) {
            synchronized (DBHelper.class) {
                if (dataSource == null) {
                    initialize();
                    String driverClass = mysqlProps.getProperty("driverClassName");
                    try {
                        Class.forName(driverClass);
                    } catch (ClassNotFoundException cnfe) {
                        throw new RuntimeException("Could not load db driver class: " + driverClass);
                    }
                    Properties props = FullPullHelper.getFullPullProperties(FullPullConstants.ZK_NODE_NAME_MYSQL_CONF, true);
                    props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_USERNAME, mysqlProps.getProperty("username"));
                    props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_PASSWORD, mysqlProps.getProperty("password"));
                    props.setProperty(FullPullConstants.DB_CONF_PROP_KEY_URL, mysqlProps.getProperty("url"));

                    DruidDataSourceProvider provider = new DruidDataSourceProvider(props);
                    dataSource = provider.provideDataSource();
                }
            }
        }
        connection = dataSource.getConnection();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * 统一资源关闭处理
     *
     * @param conn
     * @param ps
     * @param rs
     * @return
     * @throws SQLException
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("DBHelper close resource exception", e);
        }
    }

    /**
     * t_dbus_datasource 表获取 ctrlTopic
     *
     * @param dsName
     * @return
     */
    public static String getCtrlTopicFromDB(String dsName) throws Exception {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String ctrl_topic = null;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT              ");
            sql.append("d.ctrl_topic        ");
            sql.append("        FROM        ");
            sql.append("t_dbus_datasource d ");
            sql.append("WHERE               ");
            sql.append("d.ds_name = ?       ");

            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, dsName);
            rs = ps.executeQuery();
            if (rs.next()) {
                ctrl_topic = rs.getString("ctrl_topic");
                logger.info("fullpull.src.topic from database is {}", ctrl_topic);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return ctrl_topic;
    }

    public static MetaWrapper getMetaInDbus(String reqString) {
        MetaWrapper meta = new MetaWrapper();
        JSONObject reqJson = JSONObject.parseObject(reqString);
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);

        int dsId = payloadJson.getIntValue(FullPullConstants.REQ_PAYLOAD_DATA_SOURCE_ID);
        String schemaName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
        String targetTableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String sql = "SELECT tm.column_name, tm.column_id, tm.data_type, "
                    + "tm.data_length, tm.data_precision, tm.data_scale, "
                    + "tm.nullable, tm.is_pk, tm.pk_position "
                    + "FROM t_data_tables dt, t_table_meta tm "
                    + "where dt.ds_id=" + dsId
                    + " and dt.schema_name='" + schemaName
                    + "' and dt.table_name='" + targetTableName
                    + "' and tm.ver_id=dt.ver_id  "
                    + "and tm.hidden_column='NO' "
                    + "and tm.virtual_column ='NO'";
            logger.info("Query Meta sql is {}.", sql);

            conn = getDBusMgrConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();// 执行语句，得到结果集
            while (rs.next()) {
                MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();
                cell.setOwner(schemaName);
                cell.setTableName(targetTableName);
                cell.setColumnName(rs.getString("column_name"));
                cell.setColumnId(rs.getInt("column_id"));
                cell.setDataType(rs.getString("data_type"));
                cell.setDataLength(rs.getLong("data_length"));
                cell.setDataPrecision(rs.getInt("data_precision"));
                cell.setDataScale(rs.getInt("data_scale"));
                cell.setNullAble(rs.getString("nullable"));
                cell.setIsPk(rs.getString("is_pk"));
                cell.setPkPosition(rs.getInt("pk_position"));
                meta.addMetaCell(cell);
            }
            return meta;
        } catch (Exception e) {
            logger.error("Query Meta In Dbus encountered Excetpion", e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return null;
    }

    public static DBRecordReader getRecordReader(GenericConnManager dbManager, DBConfiguration dbConf,
                                                 DataDBInputSplit inputSplit, String logicalTableName) {
        try {
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            switch (dataBaseType) {
                case ORACLE:
                    return new OracleDBRecordReader(dbManager, dbConf,
                            inputSplit, dbConf.getInputFieldNames(), logicalTableName);
                case MYSQL:
                    return new DBRecordReader(dbManager, dbConf,
                            inputSplit, dbConf.getInputFieldNames(), logicalTableName);
                default:
                    return new DBRecordReader(dbManager, dbConf,
                            inputSplit, dbConf.getInputFieldNames(), logicalTableName);
            }
        } catch (Exception e) {
            logger.error("Get RecordReader encountered exception. Will return null. ", e);
            return null;
        }
    }

    public static String getSplitColumn(GenericSqlManager dbManager, DBConfiguration dbConf) {
        try {
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            switch (dataBaseType) {
                case ORACLE:
                    return ((OracleManager) dbManager).getSplitColumn();
                case MYSQL:
                    return ((MySQLManager) dbManager).getSplitColumn();
                case DB2:
                    return ((DB2Manager) dbManager).getSplitColumn();
                default:
                    return dbManager.getSplitColumn();
            }
        } catch (Exception e) {
            logger.error("Encountered exception.", e);
            return null;
        }
    }


    public static List<EncodePlugin> loadEncodePlugins() {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<EncodePlugin> plugins = new LinkedList<>();
        try {
            String sql = "select * from t_encode_plugins where status = 'active'";
            conn = getDBusMgrConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();// 执行语句，得到结果集
            while (rs.next()) {
                EncodePlugin plugin = new EncodePlugin();
                plugin.setId(rs.getLong("id") + "");
                plugin.setName(rs.getString("name"));
                plugin.setJarPath(rs.getString("path"));
                plugins.add(plugin);
            }
            logger.info("will load encode plugins.{}", plugins);
        } catch (Exception e) {
            logger.error("Query EncodePlugin In Dbus encountered Excetpion", e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return plugins;
    }

    public static String increseBatchNo(JSONObject reqJson) throws Exception {
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        int dsId = payloadJson.getIntValue(FullPullConstants.REQ_PAYLOAD_DATA_SOURCE_ID);
        String targetTableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);
        String schemaName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);

        String outputVersion = FullPullHelper.getOutputVersion();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String version = "-1";
        String batchNo = "-1";
        String sql = "";
        try {
            conn = getDBusMgrConnection();
            //  传统全量：   1.2 拉全量自增版本， 1.3 拉全量自增batch_id
            //  独立拉全量： 1.2 没有，           1.3 不自增batch_id
            if (outputVersion.equals(Constants.VERSION_12)) {
                // 版本号是否加1标志位。如果没有此标志位(旧版消息没有)，将其置为true，表示要对版本号进行加1。
                if (!payloadJson.containsKey(FullPullConstants.REQ_PAYLOAD_INCREASE_VERSION)) {
                    payloadJson.put(FullPullConstants.REQ_PAYLOAD_INCREASE_VERSION, true);
                }

                if (payloadJson.getBooleanValue(FullPullConstants.REQ_PAYLOAD_INCREASE_VERSION)) {
                    // 增版本号标志位true时，执行。（传统的与增量有互动的全量拉取，每次拉取都需要增版本号。）
                    sql = "update t_meta_version set version = version + 1 where id = (SELECT ver_id from t_data_tables " +
                            "where ds_id = ? "
                            + "and schema_name = ? "
                            + "and table_name = ? )";
                    ps = conn.prepareStatement(sql);
                    ps.setInt(1, dsId);
                    ps.setString(2, schemaName);
                    ps.setString(3, targetTableName);
                    ps.execute();
                    close(null, ps, rs);
                }
            } else {
                // 1.3 or later
                // 批次号是否加1标志位。如果没有此标志位(旧版消息没有)，将其置为true，表示要对批次号进行加1。
                if (!payloadJson.containsKey(FullPullConstants.REQ_PAYLOAD_INCREASE_BATCH_NO)) {
                    payloadJson.put(FullPullConstants.REQ_PAYLOAD_INCREASE_BATCH_NO, true);
                }
                if (payloadJson.getBooleanValue(FullPullConstants.REQ_PAYLOAD_INCREASE_BATCH_NO)) {
                    // 增批次号标志位为true时执行。（传统的与增量有互动的全量拉取，每次拉取都需要增批次号。）
                    sql = "update t_data_tables set batch_id = batch_id + 1 " +
                            "where ds_id = ? " +
                            "and schema_name = ? " +
                            "and table_name = ?";
                    ps = conn.prepareStatement(sql);
                    ps.setInt(1, dsId);
                    ps.setString(2, schemaName);
                    ps.setString(3, targetTableName);
                    ps.execute();
                    close(null, ps, rs);
                }

                //查询batch id
                sql = "SELECT batch_id FROM t_data_tables " +
                        "where ds_id = ? " +
                        "and schema_name = ? " +
                        "and table_name = ? ";
                ps = conn.prepareStatement(sql);
                ps.setInt(1, dsId);
                ps.setString(2, schemaName);
                ps.setString(3, targetTableName);
                rs = ps.executeQuery();// 执行语句，得到结果集
                while (rs.next()) {
                    batchNo = rs.getString("batch_id");
                }
                close(null, ps, rs);
            }

            // 查询version，后续会写入zk
            sql = "SELECT mv.version FROM t_data_tables dt, t_meta_version mv " +
                    "where dt.ds_id = ? " +
                    "and dt.schema_name = ? " +
                    "and dt.table_name = ? " +
                    "and dt.ver_id = mv.id";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, dsId);
            ps.setString(2, schemaName);
            ps.setString(3, targetTableName);
            // 执行语句，得到结果集
            rs = ps.executeQuery();
            while (rs.next()) {
                version = rs.getString("version");
            }
            conn.commit();
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            logger.error("Exception happened when try to get newest version from mysql db. Exception Info:", e);
            throw e;
        } finally {
            close(conn, ps, rs);
        }

        try {
            payloadJson.put(FullPullConstants.REQ_PAYLOAD_VERSION, Integer.parseInt(version));
            payloadJson.put(FullPullConstants.REQ_PAYLOAD_BATCH_NO, batchNo);
            reqJson.put(FullPullConstants.REQ_PAYLOAD, payloadJson);
        } catch (Exception e) {
            logger.error("Exception happened when Apply new version failed.");
        }
        return reqJson.toJSONString();
    }

}
