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

package com.creditease.dbus.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DBRecordReader;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.OracleDBRecordReader;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.msgencoder.EncodeColumn;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.MySQLManager;
import com.creditease.dbus.manager.OracleManager;
import com.creditease.dbus.manager.SqlManager;

public class DBHelper {
    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);
    private static Properties mysqlProps = null;
    private static final String DATASOURCE_CONFIG_NAME = Constants.COMMON_ROOT + "/" + Constants.ZkTopoConfForFullPull.MYSQL_CONFIG;
    
    private static synchronized void initialize() throws Exception {
        if(mysqlProps == null) {
            mysqlProps = PropertiesHolder.getProperties(DATASOURCE_CONFIG_NAME);
        }
    }
    
    public static Connection getMysqlConnection() throws Exception {
        initialize();
        // 2. Set up jdbc driver
        try {
            Class.forName(mysqlProps.getProperty("driverClassName"));
        } catch (ClassNotFoundException e) {
            logger.error("MySQL JDBC Driver not found. ", e);
            throw e;
        }

         // Get information about database from mysqlProps for connecting DB
         String dbAddr = mysqlProps.getProperty("url");
         String userName = mysqlProps.getProperty("username");
         String password = mysqlProps.getProperty("password");
         Connection conn = null;
         try {
             conn = DriverManager
                     .getConnection(dbAddr, userName, password);
         } catch (SQLException e) {
             logger.error("Connection to MySQL DB failed!",e);
             throw e;
         } finally {
             if (conn != null) {
                 logger.info("Make MySQL DB Connection successful. ");
             } else {
                 logger.error("Failed to make MySQL DB Connection!");
             }
         }
         return conn;
    }
    
    public static DBConfiguration generateDBConfiguration(String dataSourceInfo) {
        Properties confProperties = new Properties();
        //从源库中获取拉取目标表信息
        confProperties = getPullTargetDbInfo(dataSourceInfo);
        try {
            Properties otherConfProperties = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
            //从配置文件中获取分片大小
            Object dataPullingFetchSizeProperty = otherConfProperties.get(DBConfiguration.SPLIT_SHARD_SIZE);
            //如果配置文件中配置了分片大小，则放入confProperties
            if (dataPullingFetchSizeProperty != null) {
                confProperties.put(DBConfiguration.SPLIT_SHARD_SIZE, Integer.parseInt((String) dataPullingFetchSizeProperty));
            }
        }
        catch (Exception e1) {
            e1.printStackTrace();
        }
        DBConfiguration dbConf = null;
        SqlManager dbManager = null;
        try {
            dbConf = new DBConfiguration(confProperties);
            dbManager = FullPullHelper.getDbManager(dbConf, (String)(confProperties.get(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY)));
            String schemaName = confProperties.getProperty(DBConfiguration.INPUT_SCHEMA_PROPERTY);
            String tableName = confProperties.getProperty(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY);
            // 目标抽取列获取。对于系列表，任取其中一个表来获取列信息。此处取第一个表。
            if(tableName.indexOf(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
                tableName = tableName.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
            }
            if (null != tableName) {
                //获取我们所支持类型的列
                String[] colNames = getColumnNames(dbManager, dbConf, tableName);

                String[] sqlColNames = null;
                if (null != colNames) {
                    List<String> allSupportedCols = Arrays.asList(colNames);
                    List<String> selectedSupportedCols = new ArrayList<>();

                    String inputFieldNames = confProperties.getProperty(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
                    //如果用户指定了他想要拉取的列
                    if(StringUtils.isNotBlank(inputFieldNames)) {
                        inputFieldNames = inputFieldNames.toUpperCase();
                        //从该表，我们支持的全部列中，挑出用户指定的列
                        String[]  inputFields = inputFieldNames.split(",");
                        for (String inputField : inputFields) {
                            if(allSupportedCols.contains(inputField)){
                                selectedSupportedCols.add(inputField);
                            }
                        }
                    } else {
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

            // 表分区信息获取
            List<String> partitionsList = new ArrayList<>();

            String dsType = confProperties.getProperty(DBConfiguration.DataSourceInfo.DS_TYPE);
            String tableNameWithoutSchema = tableName.indexOf(".") != -1 ? tableName.split("\\.")[1] : tableName;
            String query = "";
            // 目前仅支持oracle分区表
            if (dsType.toUpperCase().equals(DbusDatasourceType.ORACLE.name())) {
                query = "select PARTITION_NAME from DBA_TAB_PARTITIONS where table_owner='" + schemaName
                        + "' and table_name='" + tableNameWithoutSchema + "' order by PARTITION_NAME";
            }else if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())) {
                query = "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '" + schemaName
                        + "' AND TABLE_NAME = '" + tableNameWithoutSchema + "' order by PARTITION_NAME";
            }

            logger.info("Partions query sql:{}.", query);

            if(StringUtils.isNotBlank(query)) {
                partitionsList = dbManager.queryTablePartitions(query);
            }

            if (partitionsList.isEmpty()) {
                // 无分区时，放入一个空字符串。便于调用方统一处理分区情况。否则调用方得根据是否有分区，决定是否要循环处理。逻辑会复杂。
                // 此处放入空字符串，调用方于是可以统一采用循环来处理，只不过循环只一次，且拿到的是空字符串而已。
                partitionsList.add("");
            }
            confProperties.put(DBConfiguration.TABEL_PARTITIONS, partitionsList);
            logger.info("Found [{}] partitions for table [{}].", partitionsList.size(), tableName);
        } catch(Exception e) {
            logger.error("Encountered exception when generating DBConfiguration.", e);
        } finally {
            try {
                dbManager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dbConf;
    }

    private static Properties getPullTargetDbInfo(String pullFullDataMessage) {
        JSONObject wrapperObj = JSONObject.parseObject(pullFullDataMessage);
        //获取payload信息
        JSONObject fullDataPullReqObj = wrapperObj.getJSONObject("payload");
        // JSONObject.fromObject(jsonObj.getString("fullDataPullReq"));

        //获取resultTopic
        String resultTopic = null;
        if(fullDataPullReqObj.containsKey(DataPullConstants.FULL_DATA_PULL_REQ_RESULT_TOPIC)) {
            resultTopic = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_RESULT_TOPIC);
        }
        
        int dbusDatasourceId = fullDataPullReqObj.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_DATA_SOURCE_ID);
        String schema = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
        String logicTableName = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
        Object scn = fullDataPullReqObj.get(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCN_NO);
        int seqNo = fullDataPullReqObj.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO);

        // 这些字段可以为空
        String splitCol = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_COL);
        String splitBoundingQuery = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_BOUNDING_QUERY);
        String inputConditions = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_INPUT_CONDITIONS);
        String pullTargetCols = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_PULL_TARGET_COLS);
      
        String opTs = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_OP_TS);
        
        Properties confProperties = new Properties();
        confProperties.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, schema + "." + logicTableName);
        
        String physicalTables = fullDataPullReqObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_PHYSICAL_TABLES);
        if(StringUtils.isBlank(physicalTables)) {
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
        if(scn != null && StringUtils.isNotBlank((String)scn)) {
            confProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN, Long.valueOf((String)scn));
        }
        
        confProperties.put(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SEQNO, seqNo);
        if(StringUtils.isNotBlank(splitCol)) {
            confProperties.put(DBConfiguration.INPUT_SPLIT_COL, splitCol);
        }
        
        if(StringUtils.isNotBlank(inputConditions)) {
            confProperties.put(DBConfiguration.INPUT_CONDITIONS_PROPERTY, inputConditions);
        }
        
        if(StringUtils.isNotBlank(splitBoundingQuery)) {
            confProperties.put(DBConfiguration.INPUT_BOUNDING_QUERY, splitBoundingQuery);
        }
        if(StringUtils.isNotBlank(pullTargetCols)) {
            confProperties.put(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, pullTargetCols);
        }
      
        confProperties.put(DBConfiguration.DATA_IMPORT_OP_TS, opTs);
        
        // StringBuffer sb = new StringBuffer();
        // sb.append("select ds.*, mv.db_name, mv.schema_name, mv.table_name,
        // mv.version from ");
        // sb.append("t_dbus_datasource ds, t_meta_version mv ");
        // sb.append("where mv.ds_id=ds.id and ds.id = ");
        // sb.append(dbusDatasourceId);

        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        try {
            conn = getMysqlConnection();

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

            if(ret != null) {
                ret.close();
                ret = null;
            }
            if(pst != null) {
                pst.close();
                pst = null;
            }

            //获得meta相同是否有数据
            sql = "SELECT mv.db_name, mv.id metaVersionId, mv.version, mv.schema_name, dt.output_topic " +
                    "FROM t_data_tables dt, t_meta_version mv " +
                    "WHERE dt.ds_id = ? " +
                    "and dt.schema_name = ? " +
                    "and dt.table_name = ? " +
                    "and dt.ver_id = mv.id ";
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
                String outputTopic = ret.getString("output_topic");
                
                confProperties.put(DBConfiguration.DataSourceInfo.DB_NAME, dbName);
                confProperties.put(DBConfiguration.DataSourceInfo.DB_SCHEMA, dbSchema);
                confProperties.put(DBConfiguration.DataSourceInfo.TABLE_NAME, logicTableName);
                confProperties.put(DBConfiguration.DataSourceInfo.PULLING_VERSION, version);
                //若拉全量请求指定了输出结果topic,用指定的。否则，用数据库里查到的。
                confProperties.put(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC, resultTopic != null ? resultTopic : outputTopic);
                logger.info("Out put topic is :{}", (String)(confProperties.get(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC)));
            }
            if(ret != null) {
                ret.close();
                ret = null;
            }
            if(pst != null) {
                pst.close();
                pst = null;
            }

            //计算每个row最大长度来估算prefetch行数
            sql = "SELECT sum(data_length) recordRowSize FROM t_table_meta where ver_id = ? ";

            pst = conn.prepareStatement(sql);
            pst.setInt(1, metaVersionId);
            ret = pst.executeQuery();
            if (ret.next()) {
                int recordRowSize = ret.getInt("recordRowSize");
                confProperties.put(DBConfiguration.DB_RECORD_ROW_SIZE, recordRowSize);
                logger.info("MaxRowLength is :{}", recordRowSize + "");
            }

            if(ret != null) {
                ret.close();
                ret = null;
            }
            if(pst != null) {
                pst.close();
                pst = null;
            }
            
            // 脱敏字段查询
            List<EncodeColumn> list = new ArrayList<>();             
            sql = "SELECT c.*, m.data_length FROM t_encode_columns c,t_data_tables t,t_table_meta m"
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
                list.add(col);
            }

            if(ret != null) {
                ret.close();
                ret = null;
            }
            if(pst != null) {
                pst.close();
                pst = null;
            }

            logger.info("Columns need encoding are : {}",list);
            confProperties.put(DBConfiguration.TABEL_ENCODE_COLUMNS, list); 
            confProperties.put(OracleManager.ORACLE_TIMEZONE_KEY, "GMT");
        }
        catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (ret != null)
                    ret.close();
                if (pst != null)
                    pst.close();
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return confProperties;
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
            
            conn = getMysqlConnection();
            pst = conn.prepareStatement(sql);
            rs = pst.executeQuery();// 执行语句，得到结果集
            while (rs.next()) { 
                MetaWrapper.MetaCell cell= new MetaWrapper.MetaCell();
                cell.setOwner(schema);
                cell.setTableName(targetTableName); 
                cell.setColumnName(rs.getString("column_name"));
                cell.setColumnId(rs.getInt("column_id"));
                cell.setDataType(rs.getString("data_type"));
                cell.setDataLength(rs.getInt("data_length"));
                cell.setDataPrecision(rs.getInt("data_precision"));
                cell.setDataScale(rs.getInt("data_scale"));
                cell.setNullAble(rs.getString("nullable"));
                cell.setIsPk(rs.getString("is_pk"));
                cell.setPkPosition(rs.getInt("pk_position"));
                meta.addMetaCell(cell);
            }
            return meta;
        }
        catch (Exception e) {
            logger.error("Query Meta In Dbus encountered Excetpion", e);
        }finally {
            try {
                if (rs != null)
                    rs.close();
                if (pst != null)
                    pst.close();
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                logger.error("Query Meta In Dbus encountered Excetpion", e);
            }
        }
        return null;
    }
    
    public static DBRecordReader getRecordReader(SqlManager dbManager,
                                                 DBConfiguration dbConf,
                                                 DataDrivenDBInputFormat.DataDrivenDBInputSplit inputSplit,
                                                 String logicalTableName) {
        try {
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            switch (dataBaseType) {
                case ORACLE:
                    return new OracleDBRecordReader(dbManager.getConnection(), dbConf,
                                                    inputSplit, dbConf.getInputFieldNames(), logicalTableName);
                case MYSQL:
                    return new DBRecordReader(dbManager.getConnection(), dbConf,
                                              inputSplit, dbConf.getInputFieldNames(), logicalTableName);
                default:
                    return new DBRecordReader(dbManager.getConnection(), dbConf,
                                       inputSplit, dbConf.getInputFieldNames(), logicalTableName);
            }
        }
        catch (Exception e) {
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
        }
        catch (Exception e) {
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
        }
        catch (Exception e) {
            logger.error("Encountered exception.", e);
            return null;
        }
    }
    
}
