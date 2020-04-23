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
import com.creditease.dbus.common.MultiTenancyHelper;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.SupportedMysqlDataType;
import com.creditease.dbus.commons.SupportedOraDataType;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.GenericSqlManager;
import com.creditease.dbus.manager.OracleManager;
import com.creditease.dbus.msgencoder.EncodeColumn;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class DBConfigurationHelper {
    private static Logger logger = LoggerFactory.getLogger(DBConfigurationHelper.class);

    public static DBConfiguration generateDBConfiguration(String reqString) throws Exception {

        DBConfiguration dbConf = null;
        try {
            JSONObject reqJson = JSONObject.parseObject(reqString);
            //Get common basic conf info from zk. Maybe overrided by customized conf from front-end or db
            Properties basicProperties = getBasicPropertiesFromZK();
            //从dbus管理库中获取拉取目标表信息
            basicProperties = getPullTargetDbInfoFromMgr(reqJson, basicProperties);
            dbConf = new DBConfiguration(basicProperties);
            //从源库中获取拉取目标表信息
            getPullTargetDbInfoFromSource(reqJson, dbConf, basicProperties);
            //从请求中获取配置
            getConfigFromRequest(reqJson, dbConf);
        } catch (Exception e) {
            logger.error("Exception happened when load DBConfiguration .", e);
            throw e;
        }
        return dbConf;
    }

    private static void getConfigFromRequest(JSONObject reqJson, DBConfiguration dbConf) {
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        String sinkType = StringUtils.isBlank(payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SINK_TYPE)) ?
                FullPullConstants.SINK_TYPE_KAFKA : payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SINK_TYPE);
        dbConf.putProperties(DBConfiguration.SINK_TYPE, sinkType);
        if (sinkType.equals(FullPullConstants.SINK_TYPE_HDFS)) {
            ///datahub/hdfslog/oracle.p2p.p2p/loan/8/0/0/data_initial_data/right/201906191207015680001
            String hdfsRootPath = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_HDFS_ROOT_PATH);
            if (hdfsRootPath.endsWith("/")) {
                hdfsRootPath = hdfsRootPath.substring(0, hdfsRootPath.length() - 1);
            }
            String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            String dbName = dbConf.getString(DBConfiguration.DataSourceInfo.DB_NAME);
            String alias = dbConf.getString(DBConfiguration.DataSourceInfo.DS_ALIAS_NAME);
            dbName = StringUtils.isNotBlank(alias) ? alias : dbName;

            String schemaName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
            String tableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);
            String version = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_VERSION);
            String hdfsTablePath = String.format("%s/%s.%s.%s/%s/%s/0/0/data_initial_data/right/", hdfsRootPath, dsType, dbName, schemaName, tableName, version);
            hdfsTablePath = StringUtils.replace(hdfsTablePath, "//", "/").toLowerCase();
            dbConf.putProperties(DBConfiguration.HDFS_TABLE_PATH, hdfsTablePath);
        }
    }

    private static void getPullTargetDbInfoFromSource(JSONObject reqJson, DBConfiguration dbConf, Properties confProperties) {
        GenericSqlManager dbManager = null;
        try {
            dbManager = FullPullHelper.getDbManager(dbConf, (String) (confProperties.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY)));
            String schemaName = confProperties.getProperty(DBConfiguration.INPUT_SCHEMA_PROPERTY);
            String[] physicalTables = confProperties.getProperty(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY).split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);

            if (null != physicalTables) {
                // 目标抽取列获取。对于系列表，任取其中一个表来获取列信息。此处取第一个表,即physicalTables[0]。
                //获取我们所支持类型的列
                String[] colNames = dbManager.getColumnNames(physicalTables[0]);

                String[] sqlColNames = null;
                if (null != colNames) {
                    List<String> allSupportedCols = Arrays.asList(colNames);
                    List<String> selectedSupportedCols = new ArrayList<>();
                    //用户指定的拉取列
                    String inputFieldNames = confProperties.getProperty(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
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
                    } else if (MultiTenancyHelper.isMultiTenancy(reqJson)) {
                        //多租户输出列获取
                        selectedSupportedCols = getOutPutColumns(reqJson, allSupportedCols);
                    } else {
                        //源端默认全部输出
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
                } else if (dsType.toUpperCase().equals(DbusDatasourceType.DB2.name())) {
                    query = "select datapartitionname from syscat.datapartitions t where tabschema = '" + schemaName
                            + "' and tabname = '" + tableNameWithoutSchema + "'";
                }

                logger.info("Partitions query sql : {}.", query);

                if (StringUtils.isNotBlank(query)) {
                    partitionsList = dbManager.queryTablePartitions(query);
                }

                if (partitionsList.isEmpty()) {
                    // 无分区时，放入一个空字符串。便于调用方统一处理分区情况。否则调用方得根据是否有分区，决定是否要循环处理。逻辑会复杂。
                    // 此处放入空字符串，调用方于是可以统一采用循环来处理，只不过循环只一次，且拿到的是空字符串而已。
                    partitionsList.add("");
                } else if (dsType.toUpperCase().equals(DbusDatasourceType.DB2.name())) {
                    //TODO
                    //即使没有分区db2查询出来总是有 PART0 这条数据
                    if (partitionsList.size() == 1) {
                        partitionsList.clear();
                        partitionsList.add("");
                    }
                }
                logger.info("table [{}] found [{}] partitions.", tableName, partitionsList.size());
                partitionsInfo.put(tableName, partitionsList);
            }
            confProperties.put(DBConfiguration.TABEL_PARTITIONS, partitionsInfo);
        } catch (Exception e) {
            logger.error("Encountered exception when load config from source.", e);
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
    }

    private static Properties getPullTargetDbInfoFromMgr(JSONObject reqJson, Properties basicProperties) throws Exception {
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);
        boolean isMultiTenancy = MultiTenancyHelper.isMultiTenancy(reqJson);

        int dsId = payloadJson.getIntValue(FullPullConstants.REQ_PAYLOAD_DATA_SOURCE_ID);
        String schemaName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);
        String tableNameWithschemaName = schemaName + "." + tableName;


        basicProperties.put(DBConfiguration.DATA_IMPORT_OP_TS, getOpTs(payloadJson));
        basicProperties.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableNameWithschemaName);
        basicProperties.put(DBConfiguration.INPUT_SCHEMA_PROPERTY, schemaName);
        basicProperties.put(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY, getPhysicalTables(payloadJson,
                tableNameWithschemaName, schemaName));
        if (null != payloadJson.getLong(FullPullConstants.REQ_PAYLOAD_SCN_NO)) {
            basicProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN, payloadJson.getLong(FullPullConstants.REQ_PAYLOAD_SCN_NO));
        }
        basicProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SEQNO, payloadJson.getLongValue(FullPullConstants.REQ_PAYLOAD_SEQNO));
        if (null != payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SPLIT_BOUNDING_QUERY)) {
            basicProperties.put(DBConfiguration.INPUT_BOUNDING_QUERY, payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SPLIT_BOUNDING_QUERY));
        }
        if (null != payloadJson.getString(FullPullConstants.REQ_PAYLOAD_PULL_TARGET_COLS)) {
            basicProperties.put(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, payloadJson.getString(FullPullConstants.REQ_PAYLOAD_PULL_TARGET_COLS));
        }

        //这里有三个优先级,第一优先级全量请求参数 > 第二优先级多租户配置 > 第三优先级源端配置
        //目标topic
        String resultTopic = null;
        //分片列
        String splitCol = null;
        //分片大小
        Integer splitShardSize = null;
        //分片类型
        String splitStyle = null;
        //拉全量where条件
        String inputConditions = null;

        Connection conn = DBHelper.getDBusMgrConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //从管理库中获得数据源信息
            String sql = " select td.*,tm.alias from t_dbus_datasource td " +
                    " left join t_name_alias_mapping tm  on td.id=tm.name_id and tm.type = 2 " +
                    " where td.id = ?";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, dsId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String alias = rs.getString("alias");

                if (StringUtils.isNotBlank(alias)) {
                    basicProperties.put(DBConfiguration.DataSourceInfo.DS_ALIAS_NAME, alias);
                }
                basicProperties.put(DBConfiguration.DataSourceInfo.DB_NAME, rs.getString("ds_name"));
                basicProperties.put(DBConfiguration.DataSourceInfo.DS_TYPE, rs.getString("ds_type"));
                basicProperties.put(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY, rs.getString("dbus_user"));
                basicProperties.put(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY, rs.getString("dbus_pwd"));
                basicProperties.put(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY, rs.getString("slave_url"));
                basicProperties.put(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE, rs.getString("master_url"));
            }
            DBHelper.close(null, ps, rs);

            //获得meta相同是否有数据
            sql = "SELECT mv.db_name, mv.id metaVersionId, mv.version, mv.schema_name, dt.output_topic, dt.fullpull_col," +
                    " dt.fullpull_split_shard_size, dt.fullpull_split_style ,dt.fullpull_condition " +
                    " FROM t_data_tables dt, t_meta_version mv " +
                    " WHERE dt.ds_id = " + dsId +
                    " AND dt.schema_name = '" + schemaName + "'" +
                    " AND dt.table_name = '" + tableName + "'" +
                    " AND dt.ver_id = mv.id ";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();// 执行语句，得到结果集
            int metaVersionId = 0;
            while (rs.next()) {
                metaVersionId = rs.getInt("metaVersionId");
                String dbSchema = rs.getString("schema_name");
                String version = rs.getString("version");
                basicProperties.put(DBConfiguration.DataSourceInfo.DB_SCHEMA, dbSchema);
                basicProperties.put(DBConfiguration.DataSourceInfo.TABLE_NAME, tableName);
                basicProperties.put(DBConfiguration.DataSourceInfo.PULLING_VERSION, version);

                //第三优先级源端配置获取,不管是否为null直接赋值
                //resultTopic = rs.getString("output_topic");
                splitCol = rs.getString("fullpull_col");
                splitShardSize = rs.getInt("fullpull_split_shard_size");
                splitStyle = rs.getString("fullpull_split_style");
                inputConditions = rs.getString("fullpull_condition");
            }

            DBHelper.close(null, ps, rs);
            //处理多租户相关配置,多租户配置取数据库配置
            if (isMultiTenancy) {
                basicProperties.put(FullPullConstants.REQ_PROJECT_NAME, projectJson.get(FullPullConstants.REQ_PROJECT_NAME));
                basicProperties.put(FullPullConstants.REQ_PROJECT_ID, projectJson.get(FullPullConstants.REQ_PROJECT_ID));
                basicProperties.put(FullPullConstants.REQ_PROJECT_SINK_ID, projectJson.get(FullPullConstants.REQ_PROJECT_SINK_ID));
                basicProperties.put(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID, projectJson.get(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID));


                sql = "SELECT pt.output_topic, " +
                        "pt.fullpull_col, " +
                        "pt.fullpull_split_shard_size, " +
                        "pt.fullpull_split_style, " +
                        "pt.fullpull_condition, " +
                        "t.topo_name, " +
                        "tm.alias " +
                        "FROM t_project_topo_table pt  " +
                        "join t_project_topo t on pt.topo_id=t.id " +
                        "LEFT JOIN t_name_alias_mapping tm ON pt.topo_id = tm.name_id AND tm.type = 1 " +
                        "WHERE pt.id = ?";
                ps = conn.prepareStatement(sql);
                ps.setInt(1, projectJson.getIntValue(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID));
                rs = ps.executeQuery();
                while (rs.next()) {
                    String fullpullCol = rs.getString("fullpull_col");
                    Integer fullpullSplitShardSize = rs.getInt("fullpull_split_shard_size");
                    String fullpullSplitStyle = rs.getString("fullpull_split_style");
                    String fullpullCondition = rs.getString("fullpull_condition");
                    String outputTopic = rs.getString("output_topic");


                    //第二优先级租户配置获取,非空才赋值
                    if (StringUtils.isNotBlank(outputTopic)) {
                        resultTopic = outputTopic;
                        logger.info("[full pull config ---] coverd outputTopic from t_project_topo_table is: {}", resultTopic);
                    }
                    if (StringUtils.isNotBlank(fullpullCol)) {
                        splitCol = fullpullCol;
                        logger.info("[full pull config ---] coverd splitCol from t_project_topo_table is: {}", splitCol);
                    }
                    if (fullpullSplitShardSize == null || splitShardSize.equals(0)) {
                        splitShardSize = fullpullSplitShardSize;
                        logger.info("[full pull config ---] coverd splitShardSize from t_project_topo_table is: {}", splitShardSize);
                    }
                    if (StringUtils.isNotBlank(fullpullSplitStyle)) {
                        splitStyle = fullpullSplitStyle;
                        logger.info("[full pull config ---] coverd splitStyle from t_project_topo_table is: {}", splitStyle);
                    }
                    if (StringUtils.isNotBlank(fullpullCondition)) {
                        inputConditions = fullpullCondition;
                        logger.info("[full pull config ---] coverd inputConditions from t_project_topo_table is: {}", inputConditions);
                    }

                    //处理router topo alias
                    String topoName = rs.getString("topo_name");
                    String alias = rs.getString("alias");
                    topoName = StringUtils.isNotBlank(alias) ? alias : topoName;
                    basicProperties.put(FullPullConstants.REQ_PROJECT_TOPO_NAME, topoName);
                }
            }
            DBHelper.close(null, ps, rs);

            //第一优先级全量请求参数获取
            String reqResultTopic = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_RESULTTOPIC);
            String reqSplitCol = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SPLIT_COL);
            String reqSplitShardSize = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SPLIT_SHARD_SIZE);
            String reqSplitStyle = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SPLIT_SHARD_STYLE);
            String reqInputConditions = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_INPUT_CONDITIONS);

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
                basicProperties.put(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC, resultTopic);
                logger.info("[full pull config ---] outPutTopic is: {}", resultTopic);
            } else {
                String sinkType = StringUtils.isBlank(payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SINK_TYPE)) ?
                        FullPullConstants.SINK_TYPE_KAFKA : payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SINK_TYPE);
                if (!FullPullConstants.SINK_TYPE_HDFS.equals(sinkType)) {
                    throw new RuntimeException("result topic can not be null.");
                }
            }
            if (StringUtils.isNotBlank(splitCol)) {
                basicProperties.put(DBConfiguration.INPUT_SPLIT_COL, splitCol);
                logger.info("[full pull config ---] splitCol is: {}", splitCol);
            }
            if (splitShardSize != null && splitShardSize != 0) {
                basicProperties.put(DBConfiguration.SPLIT_SHARD_SIZE, splitShardSize);
                logger.info("[full pull config ---] splitShardSize is: {}", splitShardSize);
            }
            if (StringUtils.isNotBlank(splitStyle)) {
                basicProperties.put(DBConfiguration.SPLIT_STYLE, splitStyle);
                logger.info("[full pull config ---] splitStyle is: {}", splitStyle);
            }
            if (StringUtils.isNotBlank(inputConditions)) {
                basicProperties.put(DBConfiguration.INPUT_CONDITIONS_PROPERTY, inputConditions);
                logger.info("[full pull config ---] splitConditions is: {}", inputConditions);
            }

            logger.info("[full pull config ---] outPutTopic is:{}, splitCol is:{}, splitShardSize is:{}, splitStyle is: {}, inputConditions is:{}",
                    basicProperties.get(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC), basicProperties.get(DBConfiguration.INPUT_SPLIT_COL),
                    basicProperties.get(DBConfiguration.SPLIT_SHARD_SIZE), basicProperties.get(DBConfiguration.SPLIT_STYLE),
                    basicProperties.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY));
            //resultTopic, splitCol, splitShardSize, splitStyle, inputConditions);


            //计算每个row最大长度来估算prefetch行数
            int recordRowSize = 0;
            sql = "SELECT data_length, data_type FROM t_table_meta where ver_id = ? ";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, metaVersionId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String dataType = rs.getString("data_type");
                if (SupportedOraDataType.isSupported(dataType) || SupportedMysqlDataType.isSupported(dataType)) {
                    recordRowSize += rs.getInt("data_length");
                }
            }
            basicProperties.put(DBConfiguration.DB_RECORD_ROW_SIZE, recordRowSize);
            logger.info("MaxRowLength is :{}", recordRowSize);
            DBHelper.close(null, ps, rs);

            //查询任务表对应的脱敏配置
            List<EncodeColumn> encodeColumnList = getColumnsNeedEncode(conn, dsId, schemaName, tableName, reqJson);
            basicProperties.put(DBConfiguration.TABEL_ENCODE_COLUMNS, encodeColumnList);
            basicProperties.put(OracleManager.ORACLE_TIMEZONE_KEY, "GMT");
            return basicProperties;
        } finally {
            DBHelper.close(conn, ps, rs);
        }
    }

    private static Object getPhysicalTables(JSONObject payloadJson, String tableNameWithschemaName, String schemaName) {
        //物理分区表
        String physicalTables = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_PHYSICAL_TABLES);
        if (StringUtils.isBlank(physicalTables)) {
            physicalTables = tableNameWithschemaName;
        } else {
            //分片表
            String[] physicalTablesArr = physicalTables.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
            for (int i = 0; i < physicalTablesArr.length; i++) {
                physicalTablesArr[i] = schemaName + "." + physicalTablesArr[i];
            }
            physicalTables = StringUtils.join(physicalTablesArr, Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
        }
        return physicalTables;
    }

    private static String getOpTs(JSONObject payloadJson) {
        //兼容旧版本 1.2的时间
        String opTs = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_OP_TS);
        if (StringUtils.isBlank(opTs)) {
            opTs = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_OGG_OP_TS);
            if (StringUtils.isBlank(opTs)) {
                throw new RuntimeException("读取OP_TS 失败. 检查ctrl消息.");
            }
        }
        return opTs;
    }

    private static List<String> getOutPutColumns(JSONObject reqJson, List<String> allSupportedCols) {
        Connection conn = null;
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);

        ArrayList<String> outPutCol = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = null;
        Integer output_list_type = null;
        try {
            conn = DBHelper.getDBusMgrConnection();

            //输出列的列类型：0，贴源输出表的所有列（输出列随源端schema变动而变动；1，指定固定的输出列（任何时候只输出您此时此地选定的列）
            sql = "select output_list_type from t_project_topo_table where id = ?";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, projectJson.getIntValue(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID));
            rs = ps.executeQuery();
            while (rs.next()) {
                output_list_type = rs.getInt("output_list_type");
            }
            //贴源输出表的所有列
            if (output_list_type == 0) {
                return allSupportedCols;
            } else {
                sql = "select v.column_name from t_project_topo_table_meta_version v,t_project_topo_table t " +
                        "where v.project_id=t.project_id and v.table_id = t.table_id and t.id = ? ";
                ps = conn.prepareStatement(sql);
                ps.setInt(1, projectJson.getIntValue(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID));
                rs = ps.executeQuery();
                while (rs.next()) {
                    outPutCol.add(rs.getString("column_name"));
                }
            }
        } catch (Exception e) {
            logger.error("Exception happend when get OutPutColumns", e);
            throw new RuntimeException(e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return outPutCol;
    }

    private static List<EncodeColumn> getColumnsNeedEncode(Connection conn, int dsId, String schemaName,
                                                           String tableName, JSONObject reqJson) {
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);

        List<EncodeColumn> encodeColumnList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "";
        try {
            // 源端表脱敏字段添加

            sql = "SELECT c.*, m.data_length FROM t_encode_columns c,t_data_tables t,t_table_meta m"
                    + " WHERE "
                    + "c.table_id = t.id "
                    + "AND m.ver_id = t.ver_id "
                    + "AND m.column_name = c.field_name "
                    + "AND t.ds_id = ? AND t.schema_name = ? AND t.table_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, dsId);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            rs = ps.executeQuery();
            while (rs.next()) {
                EncodeColumn col = new EncodeColumn();
                col.setId(rs.getLong("id"));
                col.setDesc(rs.getString("desc_"));
                col.setEncodeParam(rs.getString("encode_param"));
                col.setEncodeType(rs.getString("encode_type"));
                col.setTableId(rs.getLong("table_id"));
                col.setFieldName(rs.getString("field_name"));
                col.setUpdateTime(rs.getDate("update_time"));
                col.setLength(rs.getInt("data_length"));
                col.setTruncate(rs.getBoolean("truncate"));
                col.setPluginId(rs.getLong("plugin_id"));
                encodeColumnList.add(col);
            }

            //多租户脱敏查询
            if (MultiTenancyHelper.isMultiTenancy(reqJson)) {
                encodeColumnList = new ArrayList<>();
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
                ps = conn.prepareStatement(sql);
                ps.setInt(1, projectJson.getIntValue(FullPullConstants.REQ_PROJECT_TOPO_TABLE_ID));
                ps.setInt(2, projectJson.getIntValue(FullPullConstants.REQ_PROJECT_ID));
                rs = ps.executeQuery();
                while (rs.next()) {
                    EncodeColumn col = new EncodeColumn();
                    col.setId(rs.getLong("id"));
                    col.setDesc(rs.getString("desc_"));
                    col.setEncodeParam(rs.getString("encode_param"));
                    col.setEncodeType(rs.getString("encode_type"));
                    col.setTableId(rs.getLong("table_id"));
                    col.setFieldName(rs.getString("field_name"));
                    col.setUpdateTime(rs.getDate("update_time"));
                    col.setLength(rs.getInt("data_length"));
                    col.setTruncate(rs.getBoolean("truncate"));
                    col.setPluginId(rs.getLong("encode_plugin_id"));
                    encodeColumnList.add(col);
                }
            }
            logger.info("table {}.{} Columns need encoding are : {}", schemaName, tableName, encodeColumnList.toString());
        } catch (Exception e) {
            logger.error("Exception happend when get encode columns", e);
            throw new RuntimeException(e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        return encodeColumnList;
    }

    private static Properties getBasicPropertiesFromZK() {
        Properties commProps = FullPullHelper.getFullPullProperties(FullPullConstants.COMMON_CONFIG, true);
        Properties basicProperties = new Properties();
        basicProperties.putAll(commProps);
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
        return basicProperties;
    }
}
