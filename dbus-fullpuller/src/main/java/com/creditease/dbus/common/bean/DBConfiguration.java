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


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.bean;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.MultiTenancyHelper;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.helper.FullPullHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * A container for configuration property names for jobs with DB input/output.
 * The job can be configured using the static methods in this class,
 * Alternatively, the this.properties can be set in the configuration with proper values.
 */
public class DBConfiguration {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 定义内部类用于将常量分组
     */
    public static class DataSourceInfo {
        /**
         * JDBC Database access URL.
         */
        public static final String URL_PROPERTY_READ_ONLY = "jdbc.url";
        public static final String URL_PROPERTY_READ_WRITE = "jdbc.url.readWrite";

        /**
         * User name to access the database.
         */
        public static final String USERNAME_PROPERTY = "jdbc.username";

        /**
         * Password to access the database.
         */
        public static final String PASSWORD_PROPERTY = "jdbc.password";
        public static final String DS_TYPE = "dsType";
        public static final String DB_NAME = "dbName";
        public static final String DS_ALIAS_NAME = "dsAliasName";
        public static final String DB_SCHEMA = "dbSchema";
        public static final String TABLE_NAME = "tableName";
        public static final String PULLING_VERSION = "version";
        public static final String OUTPUT_TOPIC = "outputTopic";
    }

    /**
     * JDBC connection parameters.
     */
    public static final String CONNECTION_PARAMS_PROPERTY = "jdbc.params";

    /**
     * Fetch size.
     */
    public static final String PREPARE_STATEMENT_FETCH_SIZE = "prepare.statement.fetch.size";
    public static final String DB_RECORD_ROW_SIZE = "db.record.row.size";

    public static final String TABEL_ENCODE_COLUMNS = "table.encode.columns";
    public static final String TABEL_PARTITIONS = "table.partitions";

    /**
     * Input table name.
     */
    public static final String INPUT_TABLE_NAME_PROPERTY = "jdbc.input.table.name";

    /**
     * Field names in the Input table.
     */
    public static final String INPUT_FIELD_NAMES_PROPERTY = "jdbc.input.field.names";

    /**
     * WHERE clause in the input SELECT statement.
     */
    public static final String INPUT_CONDITIONS_PROPERTY = "jdbc.input.conditions";

    /**
     * ORDER BY clause in the input SELECT statement.
     */
    public static final String INPUT_ORDER_BY_PROPERTY = "jdbc.input.orderby";

    /**
     * Whole input query, exluding LIMIT...OFFSET.
     */
    public static final String INPUT_QUERY = "jdbc.input.query";

    /**
     * Input query to get the count of records.
     */
    public static final String INPUT_COUNT_QUERY = "jdbc.input.count.query";

    /**
     * Input query to get the max and min values of the jdbc.input.query.
     */
    public static final String INPUT_BOUNDING_QUERY = "jdbc.input.bounding.query";

    public static final String INPUT_SPLIT_COL = "jdbc.input.split.column";
    public static final String SPLIT_SHARD_SIZE = "split.shard.size";
    public static final String SPLIT_STYLE = "split.style";

    public static final String INPUT_SPLIT_NUM_MAPPERS = "jdbc.input.split.mappers.num";

    public static final String INPUT_SPLIT_LIMIT = "jdbc.input.split.limit";

    public static final String INPUT_SPLIT_NUM_MAPPERS_AUTO_SET_TO_ONE = "jdbc.input.split.mappers.autosettoone";

    public static final String INPUT_DB_PROCEDURE_CALL_NAME = "jdbc.input.procedure.call.name";
    public static final String ALLOW_TEXT_SPLITTER_PROPERTY = "jdbc.input.split.allow.textsplitter";
    public static final String DIRECT_IMPORT = "direct.import";
    /**
     * The name of the parameter to use for making Isolation level to be
     * read uncommitted by default for connections.
     */
    public static final String PROP_RELAXED_ISOLATION = "com.creditease.dbus.db.relaxedisolation";

    public static final String INPUT_SCHEMA_PROPERTY = "data.import.schema";

    public static final String DATA_IMPORT_CONSISTENT_READ_SCN = "data.import.consistent.read.scn";

    public static final String DATA_IMPORT_CONSISTENT_READ_SEQNO = "data.import.consistent.read.seqno";

    public static final String DATA_IMPORT_OP_TS = "data.import.op.ts";

    public static final String SINK_TYPE = "sink.type";
    public static final String HDFS_ROOT_PATH = "hdfs.root.path";
    public static final String HDFS_TABLE_PATH = "hdfs.table.path";
    public static final String HDFS_SEND_BATCH_SIZE = "hdfs.send.batch.size";

    private String[] inputFieldNames;

    private Properties properties;

    public DBConfiguration(Properties dbConf) {
        this.properties = dbConf;
    }

    public Properties getConfProperties() {
        return this.properties;
    }

    public void putProperties(String key, Object value) {
        this.properties.put(key, value);
    }

    public Integer getPrepareStatementFetchSize() {
        //mysql不支持这个参数,设置Integer.MIN_VALUE使用流式逐条获取数据
        if (getString(DataSourceInfo.DS_TYPE).equalsIgnoreCase(DbusDatasourceType.MYSQL.name())) {
            return Integer.MIN_VALUE;
        }
        int defaultFecthRecords = FullPullConstants.DEFAULT_PREPARE_STATEMENT_FETCH_RECORDS;
        try {
            Integer psFetchSizeConf = (Integer) (this.properties.get(DBConfiguration.PREPARE_STATEMENT_FETCH_SIZE));
            Integer dbRecordRowSize = (Integer) (this.properties.get(DBConfiguration.DB_RECORD_ROW_SIZE));
            logger.info("psFetchSizeConf: {}, dbRecordRowSize: {}", psFetchSizeConf, dbRecordRowSize);
            int rows = psFetchSizeConf / dbRecordRowSize;
            if (rows == 0 || rows > defaultFecthRecords) {
                return defaultFecthRecords;
            }
            return rows;
        } catch (Exception e) {
            logger.error("Exception happend when get PrepareStatement FetchSize .", e);
            return defaultFecthRecords;
        }
    }

    public String getInputTableName() {
        return (String) (this.properties.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY));
    }

    public String[] getInputFieldNames() {
        return this.inputFieldNames;
    }

    public void setInputFieldNames(String[] inputFieldNames) {
        this.inputFieldNames = inputFieldNames;
    }

    public String getInputConditions() {
        return (String) (this.properties.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY));
    }

    public String getInputOrderBy() {
        return (String) (this.properties.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY));
    }

    public void setInputOrderBy(String orderby) {
        if (orderby != null && orderby.length() > 0) {
            this.properties.put(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
        }
    }

    public String getInputCountQuery() {
        return (String) (this.properties.get(DBConfiguration.INPUT_COUNT_QUERY));
    }

    public long getInputSplitMappersNum() {
        return this.properties.get(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS) == null ? 1L : (Long) (this.properties.get(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS));
    }

    public long getInputSplitLimit() {
        return this.properties.get(DBConfiguration.INPUT_SPLIT_LIMIT) == null ? -1 : (Integer) (this.properties.get(DBConfiguration.INPUT_COUNT_QUERY));
    }

    public int getSplitShardSize() {
        return this.properties.get(DBConfiguration.SPLIT_SHARD_SIZE) == null
                ? FullPullConstants.DEFAULT_SPLIT_SHARD_SIZE
                : (Integer) this.properties.get(DBConfiguration.SPLIT_SHARD_SIZE);
    }

    public boolean getAllowTextSplitter() {
        return true;
    }

    public Object get(String propKey) {
        return this.properties.get(propKey);
    }

    public int getInt(String propKey, int defaultValue) {
        try {
            Object prop = this.properties.get(propKey);
            return prop == null ? defaultValue : (Integer) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public Boolean getBoolean(String propKey, boolean defaultValue) {
        try {
            Object prop = this.properties.get(propKey);
            return prop == null ? defaultValue : (Boolean) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public String getString(String propKey) {
        return getString(propKey, null);
    }

    public String getString(String propKey, String defaultStr) {
        return this.properties.get(propKey) != null ? (String) this.properties.get(propKey) : defaultStr;
    }

    public Long getLong(String propKey, Long defaultValue) {
        try {
            Object prop = this.properties.get(propKey);
            return prop == null ? defaultValue : (Long) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public void set(String propKey, Object value) {
        this.properties.put(propKey, value);
    }

    /**
     * zk 节点名
     *
     * @param reqJson
     * @param outputVersion
     * @return
     */
    public String buildSlashedNameSpace(JSONObject reqJson, String outputVersion) {
        Long id = FullPullHelper.getSeqNo(reqJson);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
        String timestamp = sdf.format(new Date(id));
        String dbName = this.properties.getProperty(DBConfiguration.DataSourceInfo.DB_NAME);
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        String dbSchema = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);

        if (outputVersion.equals(Constants.VERSION_12)) {
            String versionNo = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_VERSION);
            return String.format("%s/%s/%s/%s", dbName, dbSchema, tableName, versionNo);
        } else {
            String batchNo = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_BATCH_NO);
            return String.format("%s/%s/%s/%s - %s", dbName, dbSchema, tableName, timestamp, batchNo);
        }
    }

    public String buildNameSpaceForZkUidFetch(String reqString) {
        String dbName = this.properties.getProperty(DBConfiguration.DataSourceInfo.DB_NAME);
        return String.format("%s.schema.table.version", dbName);

    }

    public String getDbNameAndSchema() {
        String dbName = this.properties.getProperty(DataSourceInfo.DB_NAME);
        String dbSchema = this.properties.getProperty(DataSourceInfo.DB_SCHEMA);
        return dbName + "/" + dbSchema;
    }

    public String getKafkaKey(String reqString, String seriesTableName) {
        String dbType = this.properties.getProperty(DBConfiguration.DataSourceInfo.DS_TYPE);
        String dbName = this.properties.getProperty(DBConfiguration.DataSourceInfo.DB_NAME);
        String alias = this.properties.getProperty(DataSourceInfo.DS_ALIAS_NAME);
        if (StringUtils.isNotBlank(alias)) {
            dbName = alias;
        }
        String dbSchema = this.properties.getProperty(DBConfiguration.DataSourceInfo.DB_SCHEMA);
        String tableName = this.properties.getProperty(DBConfiguration.DataSourceInfo.TABLE_NAME);

        JSONObject reqJson = JSONObject.parseObject(reqString);
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        String version = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_VERSION);

        if (MultiTenancyHelper.isMultiTenancy(reqString)) {
            String topoName = this.properties.getProperty(FullPullConstants.REQ_PROJECT_TOPO_NAME);
            return String.format("%s.%s.%s!%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    topoName, dbSchema, tableName, version, "0", "0", System.currentTimeMillis(), "wh_placeholder");
        } else {
            return String.format("%s.%s.%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    dbSchema, tableName, version, "0", "0", System.currentTimeMillis(), "wh_placeholder");
        }
    }

    public String getDbTypeAndNameSpace(String reqString, String seriesTableName) {
        String dbType = this.properties.getProperty(DBConfiguration.DataSourceInfo.DS_TYPE);
        String dbName = this.properties.getProperty(DBConfiguration.DataSourceInfo.DB_NAME);
        String alias = this.properties.getProperty(DataSourceInfo.DS_ALIAS_NAME);
        if (StringUtils.isNotBlank(alias)) {
            dbName = alias;
        }
        JSONObject reqJson = JSONObject.parseObject(reqString);
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        String dbSchema = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);
        String version = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_VERSION);

        //if (seriesTableName.indexOf(".") != -1) {
        //    seriesTableName = seriesTableName.split("\\.")[1];// 传入参数可能是schema.seriesTableName形式。需要剔除schema信息
        //}
        //if (seriesTableName.equals(tableName)) {
        //    // 只有系列表，才需要将系列表名放置到namespace末级。
        //    // 如果seriesTableName和输出表名一致，表明是本表，而非系列表，partition部分需置为"0"。
        //    seriesTableName = "0";
        //}

        if (MultiTenancyHelper.isMultiTenancy(reqString)) {
            String topoName = this.properties.getProperty(FullPullConstants.REQ_PROJECT_TOPO_NAME);
            return String.format("%s.%s!%s.%s.%s.%s.%s.%s", dbType, dbName, topoName, dbSchema, tableName, version, "0", "0");
        } else {
            return String.format("%s.%s.%s.%s.%s.%s.%s", dbType, dbName, dbSchema, tableName, version, "0", "0");
        }
    }

}
