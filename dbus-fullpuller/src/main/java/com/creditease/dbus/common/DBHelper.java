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

package com.creditease.dbus.common;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DBRecordReader;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.OracleDBRecordReader;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.SupportedMysqlDataType;
import com.creditease.dbus.commons.SupportedOraDataType;
import com.creditease.dbus.dbaccess.DruidDataSourceProvider;
import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.*;
import com.creditease.dbus.msgencoder.EncodeColumn;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DBHelper {
    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    private static Properties mysqlProps = null;
    private static final String DATASOURCE_CONFIG_NAME = Constants.COMMON_ROOT + "/" + Constants.ZkTopoConfForFullPull.MYSQL_CONFIG;
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
    public static Connection getDBusMgrConnection() throws SQLException, Exception {
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
                    Properties props = FullPullHelper.getFullPullProperties(DataPullConstants.ZK_NODE_NAME_MYSQL_CONF, true);
                    props.setProperty(Constants.DB_CONF_PROP_KEY_USERNAME, mysqlProps.getProperty("username"));
                    props.setProperty(Constants.DB_CONF_PROP_KEY_PASSWORD, mysqlProps.getProperty("password"));
                    props.setProperty(Constants.DB_CONF_PROP_KEY_URL, mysqlProps.getProperty("url"));

                    DruidDataSourceProvider provider = new DruidDataSourceProvider(props);
                    dataSource = provider.provideDataSource();
                }
            }
        }
        connection = dataSource.getConnection();
        return connection;
    }

    /**
     * 统一资源关闭处理
     *
     * @param conn
     * @param pst
     * @param ret
     * @return
     * @throws SQLException
     */
    public static void close(Connection conn, PreparedStatement pst, ResultSet ret) {
        try {
            if (ret != null) {
                ret.close();
            }
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("DBHelper close resource exception", e);
        }
    }

    public static DBConfiguration generateDBConfiguration(String dataSourceInfo) {
        // Get common basic conf info from zk. Maybe overrided by customized conf from front-end or db
        Properties basicProperties = new Properties();
        try {
            Properties commProps = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
            basicProperties = new Properties(commProps);

            //从配置文件中获取分片大小
            String splitShardSize = basicProperties.getProperty(DBConfiguration.SPLIT_SHARD_SIZE);
            //如果配置文件中配置了分片大小，更新为数字
            if (splitShardSize != null) {
                basicProperties.put(DBConfiguration.SPLIT_SHARD_SIZE, Integer.parseInt(splitShardSize));
            }
            String prepareFetchSize = basicProperties.getProperty(DBConfiguration.PREPARE_STATEMENT_FETCH_SIZE);
            //如果配置文件中配置了prepareFetchSize，更新为数字
            if (prepareFetchSize != null) {
                basicProperties.put(DBConfiguration.PREPARE_STATEMENT_FETCH_SIZE, Integer.parseInt(prepareFetchSize));
            }
        } catch (Exception e) {
            logger.error("Exception when load basicProperties .", e);
            throw e;
        }

        //从源库中获取拉取目标表信息
        Properties confProperties = getPullTargetDbInfo(dataSourceInfo, basicProperties);
        DBConfiguration dbConf = null;
        GenericJdbcManager dbManager = null;
        try {
            dbConf = new DBConfiguration(confProperties);
            dbManager = FullPullHelper.getDbManager(dbConf, (String) (confProperties.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY)));
            String schemaName = confProperties.getProperty(DBConfiguration.INPUT_SCHEMA_PROPERTY);
            String[] physicalTables = confProperties.getProperty(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY).split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);

            if (null != physicalTables) {
                // 目标抽取列获取。对于系列表，任取其中一个表来获取列信息。此处取第一个表,即physicalTables[0]。
                //获取我们所支持类型的列
                String[] colNames = getColumnNames(dbManager, dbConf, physicalTables[0]);

                String[] sqlColNames = null;
                if (null != colNames) {
                    List<String> allSupportedCols = Arrays.asList(colNames);
                    List<String> selectedSupportedCols = new ArrayList<>();

                    String inputFieldNames = confProperties.getProperty(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
                    //如果用户指定了他想要拉取的列
                    //查询任务表对应的输出列配置
                    if (StringUtils.isNotBlank(inputFieldNames)) {
                        inputFieldNames = inputFieldNames.toUpperCase();
                        //从该表，我们支持的全部列中，挑出用户指定的列
                        String[] inputFields = inputFieldNames.split(",");
                        for (String inputField : inputFields) {
                            if (allSupportedCols.contains(inputField)) {
                                selectedSupportedCols.add(inputField);
                            }
                        }
                    } else if (FullPullHelper.isDbusKeeper(dataSourceInfo)) {//多租户否?
                        selectedSupportedCols = getOutPutColumns(dataSourceInfo, allSupportedCols);
                    } else {
                        //源端默认输出
                        selectedSupportedCols = allSupportedCols;
                    }

                    int colCount = selectedSupportedCols.size();
                    sqlColNames = new String[colCount];
                    for (int i = 0; i < colCount; i++) {
                        sqlColNames[i] = dbManager.escapeColName(selectedSupportedCols.get(i));
                    }
                }
                dbConf.setInputFieldNames(sqlColNames);
            }

            String dsType = confProperties.getProperty(DBConfiguration.DataSourceInfo.DS_TYPE);
            Map partitionsInfo = new HashMap<>();

            // 表分区信息获取
            for (String tableName : physicalTables) {
                List<String> partitionsList = new ArrayList<>();
                String tableNameWithoutSchema = tableName.indexOf(".") != -1 ? tableName.split("\\.")[1] : tableName;
                String query = "";
                // 目前仅支持oracle分区表
                if (dsType.toUpperCase().equals(DbusDatasourceType.ORACLE.name())) {
                    query = "select PARTITION_NAME from DBA_TAB_PARTITIONS where table_owner='" + schemaName
                            + "' and table_name='" + tableNameWithoutSchema + "' order by PARTITION_NAME";
                } else if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())) {
                    query = "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '" + schemaName
                            + "' AND TABLE_NAME = '" + tableNameWithoutSchema + "' order by PARTITION_NAME";
                }

                logger.info("Partitions query sql:{}.", query);

                if (StringUtils.isNotBlank(query)) {
                    partitionsList = dbManager.queryTablePartitions(query);
                }

                if (partitionsList.isEmpty()) {
                    // 无分区时，放入一个空字符串。便于调用方统一处理分区情况。否则调用方得根据是否有分区，决定是否要循环处理。逻辑会复杂。
                    // 此处放入空字符串，调用方于是可以统一采用循环来处理，只不过循环只一次，且拿到的是空字符串而已。
                    partitionsList.add("");
                }
                logger.info("table [{}] found [{}] partitions.", tableName, partitionsList.size());
                partitionsInfo.put(tableName, partitionsList);
            }
            confProperties.put(DBConfiguration.TABEL_PARTITIONS, partitionsInfo);
        } catch (Exception e) {
            logger.error("Encountered exception when generating DBConfiguration.", e);
            throw e;
        } finally {
            try {
                if (dbManager != null) {
                    dbManager.close();
                }
            } catch (Exception e) {
                logger.error("close dbManager error.", e);
            }
        }
        return dbConf;
    }

    private static Properties getPullTargetDbInfo(String pullFullDataMessage, Properties confProperties) {
        JSONObject wrapperObj = JSONObject.parseObject(pullFullDataMessage);
        //获取payload信息
        JSONObject fullDataPullReqObj = wrapperObj.getJSONObject("payload");
        // JSONObject.fromObject(jsonObj.getString("fullDataPullReq"));
        JSONObject pullFullDataProject = wrapperObj.getJSONObject("project");
        boolean isKeeper = FullPullHelper.isDbusKeeper(pullFullDataMessage);
        int dbusDatasourceId = fullDataPullReqObj.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_DATA_SOURCE_ID);
        String schema = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
        String logicTableName = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
        Object scn = fullDataPullReqObj.get(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCN_NO);
        long seqNo = fullDataPullReqObj.getLongValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO);

        String splitBoundingQuery = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_BOUNDING_QUERY);
        String pullTargetCols = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_PULL_TARGET_COLS);

        //兼容旧版本 1.2的时间
        String opTs = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_OP_TS);
        confProperties.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, schema + "." + logicTableName);

        String physicalTables = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_PHYSICAL_TABLES);
        if (StringUtils.isBlank(physicalTables)) {
            physicalTables = schema + "." + logicTableName;
        } else {
            //分片表
            String[] physicalTablesArr = physicalTables.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
            for (int i = 0; i < physicalTablesArr.length; i++) {
                physicalTablesArr[i] = schema + "." + physicalTablesArr[i];
            }
            physicalTables = StringUtils.join(physicalTablesArr, Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
        }

        confProperties.put(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY, physicalTables);
        confProperties.put(DBConfiguration.INPUT_SCHEMA_PROPERTY, schema);
        if (scn != null && StringUtils.isNotBlank((String) scn)) {
            confProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN, Long.valueOf((String) scn));
        }
        confProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SEQNO, seqNo);

        if (StringUtils.isNotBlank(splitBoundingQuery)) {
            confProperties.put(DBConfiguration.INPUT_BOUNDING_QUERY, splitBoundingQuery);
        }
        if (StringUtils.isNotBlank(pullTargetCols)) {
            confProperties.put(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, pullTargetCols);
        }

        confProperties.put(DBConfiguration.DATA_IMPORT_OP_TS, opTs);

        // StringBuffer sb = new StringBuffer();
        // sb.append("select ds.*, mv.db_name, mv.schema_name, mv.table_name,
        // mv.version from ");
        // sb.append("t_dbus_datasource ds, t_meta_version mv ");
        // sb.append("where mv.ds_id=ds.id and ds.id = ");
        // sb.append(dbusDatasourceId);
        // 这些字段可以为空
        //这里有三个优先级,第一优先级全量请求参数 > 第二优先级多租户配置 > 第三优先级源端配置
        String resultTopic = null;
        String splitCol = null;
        Integer splitShardSize = null;
        String splitStyle = null;
        String inputConditions = null;

        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        try {
            conn = getDBusMgrConnection();

            //从管理库中获得数据源信息
            String sql = "select * from t_dbus_datasource where id = ? ";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, dbusDatasourceId);
            ret = pst.executeQuery();// 执行语句，得到结果集
            while (ret.next()) { // Todo 优化
                String dsName = ret.getString("ds_name");
                String dsType = ret.getString("ds_type");
                // String topic = ret.getString("topic");
                String masterUrl = ret.getString("master_url");
                String slaveUrl = ret.getString("slave_url");
                String dbusUser = ret.getString("dbus_user");
                String dbusPwd = ret.getString("dbus_pwd");

                confProperties.put(DBConfiguration.DataSourceInfo.DB_NAME, dsName);
                confProperties.put(DBConfiguration.DataSourceInfo.DS_TYPE, dsType);
                confProperties.put(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY, dbusUser);
                confProperties.put(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY, dbusPwd);
                confProperties.put(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY, slaveUrl);
                confProperties.put(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE, masterUrl);
            }

            DBHelper.close(null, pst, ret);

            //获得meta相同是否有数据
            sql = "SELECT mv.db_name, mv.id metaVersionId, mv.version, mv.schema_name, dt.output_topic, dt.fullpull_col, dt.fullpull_split_shard_size, dt.fullpull_split_style " +
                    "FROM t_data_tables dt, t_meta_version mv " +
                    "WHERE dt.ds_id = ? " +
                    "AND dt.schema_name = ? " +
                    "AND dt.table_name = ? " +
                    "AND dt.ver_id = mv.id ";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, dbusDatasourceId);
            pst.setString(2, schema);
            pst.setString(3, logicTableName);
            ret = pst.executeQuery();// 执行语句，得到结果集
            int metaVersionId = 0;
            while (ret.next()) {
                metaVersionId = ret.getInt("metaVersionId");
                String dbName = ret.getString("db_name");
                String dbSchema = ret.getString("schema_name");
                String version = ret.getString("version");
                confProperties.put(DBConfiguration.DataSourceInfo.DB_NAME, dbName);
                confProperties.put(DBConfiguration.DataSourceInfo.DB_SCHEMA, dbSchema);
                confProperties.put(DBConfiguration.DataSourceInfo.TABLE_NAME, logicTableName);
                confProperties.put(DBConfiguration.DataSourceInfo.PULLING_VERSION, version);

                //第三优先级源端配置获取
                resultTopic = ret.getString("output_topic");
                splitCol = ret.getString("fullpull_col");
                splitShardSize = ret.getInt("fullpull_split_shard_size");
                splitStyle = ret.getString("fullpull_split_style");
            }
            DBHelper.close(null, pst, ret);

            //多租户相关处理resultTopic
            if (isKeeper) {
                //resultTopic处理
                sql = "SELECT  " +
                        "output_topic, " +
                        "fullpull_col, " +
                        "fullpull_split_shard_size, " +
                        "fullpull_split_style, " +
                        "fullpull_condition " +
                        "FROM " +
                        "t_project_topo_table " +
                        "WHERE id = ?";
                //sql = "SELECT output_topic FROM t_project_topo_table WHERE id = ?";
                pst = conn.prepareStatement(sql);
                pst.setInt(1, pullFullDataProject.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID));
                ret = pst.executeQuery();
                while (ret.next()) {
                    confProperties.put(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC, ret.getString("output_topic"));
                    String output_topic = ret.getString("output_topic");
                    String fullpull_col = ret.getString("fullpull_col");
                    Integer fullpull_split_shard_size = ret.getInt("fullpull_split_shard_size");
                    String fullpull_split_style = ret.getString("fullpull_split_style");
                    String fullpull_condition = ret.getString("fullpull_condition");

                    //第二优先级租户配置获取
                    resultTopic = StringUtils.isNotBlank(output_topic) ? output_topic : resultTopic;
                    splitCol = StringUtils.isNotBlank(fullpull_col) ? fullpull_col : splitCol;
                    splitShardSize = (fullpull_split_shard_size != null && !fullpull_split_shard_size.equals(0)) ? fullpull_split_shard_size : splitShardSize;
                    splitStyle = StringUtils.isNotBlank(fullpull_split_style) ? fullpull_split_style : splitStyle;
                    inputConditions = StringUtils.isNotBlank(fullpull_condition) ? fullpull_condition : inputConditions;
                }
                DBHelper.close(null, pst, ret);

                //多租户相关处理project
                confProperties.put(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_NAME,
                        pullFullDataProject.get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_NAME));
                confProperties.put(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID,
                        pullFullDataProject.get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID));
                confProperties.put(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_SINK_ID,
                        pullFullDataProject.get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_SINK_ID));
                confProperties.put(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID,
                        pullFullDataProject.get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID));
            }

            //第一优先级全量请求参数获取
            String reqResultTopic = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_RESULT_TOPIC);
            String reqSplitCol = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_COL);
            String reqSplitShardSize = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_SHARD_SIZE);
            String reqSplitStyle = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_STYLE);
            String reqInputConditions = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_INPUT_CONDITIONS);

            if (StringUtils.isNotBlank(reqResultTopic)) {
                resultTopic = reqResultTopic;
                logger.info("[full pull config ---] coverd outPutTopic from fullpull request is: {}", resultTopic);
            }
            if (StringUtils.isNotBlank(reqSplitCol)) {
                splitCol = reqSplitCol;
                logger.info("[full pull config ---] coverd splitCol from fullpull request is: {}", splitCol);
            }
            if (StringUtils.isNotBlank(reqSplitShardSize)) {
                splitShardSize = Integer.parseInt(reqSplitShardSize);
                logger.info("[full pull config ---] coverd splitShardSize from fullpull request is: {}", splitShardSize);
            }
            if (StringUtils.isNotBlank(reqSplitStyle)) {
                splitStyle = reqSplitStyle;
                logger.info("[full pull config ---] coverd splitStyle from fullpull request is: {}", splitStyle);
            }
            if (StringUtils.isNotBlank(reqInputConditions)) {
                inputConditions = reqInputConditions;
                logger.info("[full pull config ---] coverd inputConditions from fullpull request is: {}", inputConditions);
            }

            //最终配置放入confProperties
            if (StringUtils.isNotBlank(resultTopic)) {
                confProperties.put(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC, resultTopic);
                logger.info("[full pull config ---] outPutTopic is: {}", resultTopic);
            }
            if (StringUtils.isNotBlank(splitCol)) {
                confProperties.put(DBConfiguration.INPUT_SPLIT_COL, splitCol);
                logger.info("[full pull config ---] splitCol is: {}", splitCol);
            }
            if (splitShardSize != null && splitShardSize != 0) {
                confProperties.put(DBConfiguration.SPLIT_SHARD_SIZE, splitShardSize);
                logger.info("[full pull config ---] splitShardSize is: {}", splitShardSize);
            }
            if (StringUtils.isNotBlank(splitStyle)) {
                confProperties.put(DBConfiguration.SPLIT_STYLE, splitStyle);
                logger.info("[full pull config ---] splitStyle is: {}", splitStyle);
            }
            if (StringUtils.isNotBlank(inputConditions)) {
                confProperties.put(DBConfiguration.INPUT_CONDITIONS_PROPERTY, inputConditions);
                logger.info("[full pull config ---] splitConditions is: {}", inputConditions);
            }
            logger.info("[full pull config ---]  outPutTopic is :{}, splitCol is:{}, splitShardSize is:{}, splitStyle is: {}, inputConditions is:{}",
                    resultTopic, splitCol, splitShardSize, splitStyle, inputConditions);

            int recordRowSize = 0;
            //计算每个row最大长度来估算prefetch行数
            sql = "SELECT data_length, data_type FROM t_table_meta where ver_id = ? ";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, metaVersionId);
            ret = pst.executeQuery();
            while (ret.next()) {
                String dataType = ret.getString("data_type");
                if (SupportedOraDataType.isSupported(dataType) || SupportedMysqlDataType.isSupported(dataType)) {
                    recordRowSize += ret.getInt("data_length");
                }
            }
            confProperties.put(DBConfiguration.DB_RECORD_ROW_SIZE, recordRowSize);
            logger.info("MaxRowLength is :{}", recordRowSize + "");

            DBHelper.close(null, pst, ret);
            //查询任务表对应的脱敏配置
            getColumnsNeedEncode(conn, dbusDatasourceId, schema, logicTableName, pullFullDataMessage, confProperties);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            DBHelper.close(conn, pst, ret);
        }
        return confProperties;
    }

    private static void getColumnsNeedEncode(Connection conn, int dbusDatasourceId, String schema, String logicTableName,
                                             String pullFullDataMessage, Properties confProperties) {
        JSONObject wrapperObj = JSONObject.parseObject(pullFullDataMessage);
        JSONObject pullFullDataProject = wrapperObj.getJSONObject("project");

        boolean isKeeper = FullPullHelper.isDbusKeeper(pullFullDataMessage);
        PreparedStatement pst = null;
        ResultSet ret = null;
        String sql = "";
        try {
            // 源端表脱敏字段添加
            List<EncodeColumn> list = new ArrayList<>();
            sql = "SELECT c.*, m.data_length FROM t_dba_encode_columns c,t_data_tables t,t_table_meta m"
                    + " WHERE "
                    + "c.table_id = t.id "
                    + "AND m.ver_id = t.ver_id "
                    + "AND m.column_name = c.field_name "
                    + "AND t.ds_id = ? AND t.schema_name = ? AND t.table_name = ?";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, dbusDatasourceId);
            pst.setString(2, schema);
            pst.setString(3, logicTableName);
            ret = pst.executeQuery();
            while (ret.next()) {
                EncodeColumn col = new EncodeColumn();
                col.setId(ret.getLong("id"));
                col.setDesc(ret.getString("desc_"));
                col.setEncodeParam(ret.getString("encode_param"));
                col.setEncodeType(ret.getString("encode_type"));
                col.setTableId(ret.getLong("table_id"));
                col.setFieldName(ret.getString("field_name"));
                col.setUpdateTime(ret.getDate("update_time"));
                col.setLength(ret.getInt("data_length"));
                col.setTruncate(ret.getBoolean("truncate"));
                col.setPluginId(ret.getLong("plugin_id"));
                list.add(col);
            }

            //多租户版本
            if (isKeeper) {
                // 普通用户配置脱敏字段添加
                sql = "SELECT h.*, m.data_length,tt.table_id " +
                        "FROM t_project_topo_table_encode_output_columns h,t_data_tables t,t_table_meta m , t_project_topo_table tt " +
                        "WHERE m.ver_id = t.ver_id " +
                        "AND t.id = tt.table_id " +
                        "AND m.column_name = h.field_name " +
                        "AND tt.id = h.project_topo_table_id " +
                        "AND h.encode_plugin_id is not null " +
                        "AND h.special_approve = 0 " +
                        "AND h.project_topo_table_id = ? " +
                        "AND tt.project_id = ? ";

                pst = conn.prepareStatement(sql);
                pst.setInt(1, pullFullDataProject.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID));
                pst.setInt(2, pullFullDataProject.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID));
                ret = pst.executeQuery();
                while (ret.next()) {
                    EncodeColumn col = new EncodeColumn();
                    col.setId(ret.getLong("id"));
                    col.setDesc(ret.getString("desc_"));
                    col.setEncodeParam(ret.getString("encode_param"));
                    col.setEncodeType(ret.getString("encode_type"));
                    col.setTableId(ret.getLong("table_id"));
                    col.setFieldName(ret.getString("field_name"));
                    col.setUpdateTime(ret.getDate("update_time"));
                    col.setLength(ret.getInt("data_length"));
                    col.setTruncate(ret.getBoolean("truncate"));
                    col.setPluginId(ret.getLong("encode_plugin_id"));
                    list.add(col);
                }
            }
            logger.info("Columns need encoding are : {}", list.toString());
            confProperties.put(DBConfiguration.TABEL_ENCODE_COLUMNS, list);
            confProperties.put(OracleManager.ORACLE_TIMEZONE_KEY, "GMT");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            close(conn, pst, ret);
        }
    }

    private static List<String> getOutPutColumns(String pullFullDataMessage, List<String> allSupportedCols) {
        Connection conn = null;
        JSONObject wrapperObj = JSONObject.parseObject(pullFullDataMessage);
        JSONObject pullFullDataProject = wrapperObj.getJSONObject("project");

        ArrayList<String> outPutCol = new ArrayList<>();
        PreparedStatement pst = null;
        ResultSet ret = null;
        String sql = null;
        Integer output_list_type = null;
        try {
            conn = getDBusMgrConnection();

            //输出列的列类型：0，贴源输出表的所有列（输出列随源端schema变动而变动；1，指定固定的输出列（任何时候只输出您此时此地选定的列）
            sql = "select output_list_type from t_project_topo_table where id = ?";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, pullFullDataProject.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID));
            ret = pst.executeQuery();
            while (ret.next()) {
                output_list_type = ret.getInt("output_list_type");
            }
            //贴源输出表的所有列
            if (output_list_type == 0) {
                return allSupportedCols;
            } else {
                sql = "select v.column_name from t_project_topo_table_meta_version v,t_project_topo_table t " +
                        "where v.project_id=t.project_id and v.table_id = t.table_id and t.id = ? ";
                pst = conn.prepareStatement(sql);
                pst.setInt(1, pullFullDataProject.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID));
                ret = pst.executeQuery();
                while (ret.next()) {
                    outPutCol.add(ret.getString("column_name"));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            DBHelper.close(conn, pst, ret);
        }
        return outPutCol;
    }

    public static MetaWrapper getMetaInDbus(String dataSourceInfo) {
        MetaWrapper meta = new MetaWrapper();
        JSONObject wrapperObj = JSONObject.parseObject(dataSourceInfo);
        JSONObject fullDataPullReqObj = wrapperObj.getJSONObject("payload");// JSONObject.fromObject(jsonObj.getString("fullDataPullReq"));

        int dbusDatasourceId = fullDataPullReqObj.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_DATA_SOURCE_ID);
        String schema = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
        String targetTableName = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);

        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;

        try {
            String sql = "SELECT tm.column_name, tm.column_id, tm.data_type, "
                    + "tm.data_length, tm.data_precision, tm.data_scale, "
                    + "tm.nullable, tm.is_pk, tm.pk_position "
                    + "FROM t_data_tables dt, t_table_meta tm "
                    + "where dt.ds_id=" + dbusDatasourceId
                    + " and dt.schema_name='" + schema
                    + "' and dt.table_name='" + targetTableName
                    + "' and tm.ver_id=dt.ver_id  "
                    + "and tm.hidden_column='NO' "
                    + "and tm.virtual_column ='NO'";
            logger.info("[Mysql manager] Meta query sql is {}.", sql);

            conn = getDBusMgrConnection();
            pst = conn.prepareStatement(sql);
            rs = pst.executeQuery();// 执行语句，得到结果集
            while (rs.next()) {
                MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();
                cell.setOwner(schema);
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
            DBHelper.close(conn, pst, rs);
        }
        return null;
    }

    public static DBRecordReader getRecordReader(GenericJdbcManager dbManager,
                                                 DBConfiguration dbConf,
                                                 DataDrivenDBInputFormat.DataDrivenDBInputSplit inputSplit,
                                                 String logicalTableName) {
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

    public static String getSplitColumn(SqlManager dbManager, DBConfiguration dbConf) {
        try {
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            switch (dataBaseType) {
                case ORACLE:
                    return ((OracleManager) dbManager).getSplitColumn();
                case MYSQL:
                    return ((MySQLManager) dbManager).getSplitColumn();
                default:
                    return dbManager.getSplitColumn();
            }
        } catch (Exception e) {
            logger.error("Encountered exception.", e);
            return null;
        }
    }

    private static String[] getColumnNames(SqlManager dbManager, DBConfiguration dbConf, String tableName) {
        try {
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            switch (dataBaseType) {
                case ORACLE:
                    return ((OracleManager) dbManager).getColumnNames(tableName);
                case MYSQL:
                    return ((MySQLManager) dbManager).getColumnNames(tableName);
                default:
                    return dbManager.getColumnNames(tableName);
            }
        } catch (Exception e) {
            logger.error("Encountered exception.", e);
            return null;
        }
    }

    public static List<EncodePlugin> loadEncodePlugins() {
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<EncodePlugin> plugins = new LinkedList<>();
        try {
            String sql = "select * from t_encode_plugins where status = 'active'";
            conn = getDBusMgrConnection();
            pst = conn.prepareStatement(sql);
            rs = pst.executeQuery();// 执行语句，得到结果集
            while (rs.next()) {
                EncodePlugin plugin = new EncodePlugin();
                plugin.setId(rs.getLong("id") + "");
                plugin.setName(rs.getString("name"));
                plugin.setJarPath(rs.getString("path"));
                plugins.add(plugin);
            }
            logger.info("load encode plugins.{}", plugins);
        } catch (Exception e) {
            logger.error("Query EncodePlugin In Dbus encountered Excetpion", e);
        } finally {
            DBHelper.close(conn, pst, rs);
        }
        return plugins;
    }
}
