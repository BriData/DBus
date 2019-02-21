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
package com.creditease.dbus.common.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.commons.Constants;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.commons.DbusMessage;

/**
 * A container for configuration property names for jobs with DB input/output.
 *
 * The job can be configured using the static methods in this class,
 * {@link DBInputFormat}, and {@link DBOutputFormat}.
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 *
 * @see DBConfiguration#configureDB(Properties, String, String, String,
 * String)
 * @see DBInputFormat#setInput(Job, Class, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String, String, String...)
 * @see DBOutputFormat#setOutput(Job, String, String...)
 */
public class DBConfiguration {

    private Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * 定义内部类用于将常量分组
     */
    public static class DataSourceInfo {
        /** JDBC Database access URL. */
        public static final String URL_PROPERTY_READ_ONLY = "jdbc.url";
        public static final String URL_PROPERTY_READ_WRITE = "jdbc.url.readWrite";

        /** User name to access the database. */
        public static final String USERNAME_PROPERTY = "jdbc.username";

        /** Password to access the database. */
        public static final String PASSWORD_PROPERTY = "jdbc.password";
        //  private static final Text PASSWORD_SECRET_KEY =
        //        new Text(DBConfiguration.PASSWORD_PROPERTY);
        public static final String DS_TYPE = "dsType";
        public static final String DB_NAME = "dbName";
        public static final String DB_SCHEMA = "dbSchema";
        public static final String TABLE_NAME = "tableName";
        public static final String PULLING_VERSION = "version";
        public static final String OUTPUT_TOPIC = "outputTopic";
    }

    /** JDBC connection parameters. */
    public static final String CONNECTION_PARAMS_PROPERTY = "jdbc.params";

    /** Fetch size. */
    public static final String PREPARE_STATEMENT_FETCH_SIZE = "prepare.statement.fetch.size";
    public static final String DB_RECORD_ROW_SIZE = "db.record.row.size";

    public static final String TABEL_ENCODE_COLUMNS = "table.encode.columns";
    public static final String TABEL_PARTITIONS = "table.partitions";

    /** Input table name. */
    public static final String INPUT_TABLE_NAME_PROPERTY = "jdbc.input.table.name";

    /** Field names in the Input table. */
    public static final String INPUT_FIELD_NAMES_PROPERTY = "jdbc.input.field.names";

    /** WHERE clause in the input SELECT statement. */
    public static final String INPUT_CONDITIONS_PROPERTY = "jdbc.input.conditions";

    /** ORDER BY clause in the input SELECT statement. */
    public static final String INPUT_ORDER_BY_PROPERTY = "jdbc.input.orderby";

    /** Whole input query, exluding LIMIT...OFFSET. */
    public static final String INPUT_QUERY = "jdbc.input.query";

    /** Input query to get the count of records. */
    public static final String INPUT_COUNT_QUERY = "jdbc.input.count.query";

    /** Input query to get the max and min values of the jdbc.input.query. */
    public static final String INPUT_BOUNDING_QUERY = "jdbc.input.bounding.query";

    /**
     * source table fullpull param
     */
    public static final String INPUT_SPLIT_COL = "jdbc.input.split.column";
    public static final String SPLIT_SHARD_SIZE = "split.shard.size";
    public static final String SPLIT_STYLE = "split.style";
    //public static final String SPLIT_CONDITION = "split.condition";


    public static final String INPUT_SPLIT_NUM_MAPPERS = "jdbc.input.split.mappers.num";

    public static final String INPUT_SPLIT_LIMIT = "jdbc.input.split.limit";

    public static final String INPUT_SPLIT_NUM_MAPPERS_AUTO_SET_TO_ONE = "jdbc.input.split.mappers.autosettoone";

    public static final String INPUT_DB_PROCEDURE_CALL_NAME = "jdbc.input.procedure.call.name";
    public static final String ALLOW_TEXT_SPLITTER_PROPERTY = "jdbc.input.split.allow.textsplitter";
    public static final String DIRECT_IMPORT = "direct.import";

    /** Class name implementing DBWritable which will hold input tuples. */
//  public static final String INPUT_CLASS_PROPERTY =
//    "jdbc.input.class";

    /** Output table name. */
    public static final String OUTPUT_TABLE_NAME_PROPERTY = "jdbc.output.table.name";

    /** Field names in the Output table. */
    public static final String OUTPUT_FIELD_NAMES_PROPERTY = "jdbc.output.field.names";

    /** Number of fields in the Output table. */
    public static final String OUTPUT_FIELD_COUNT_PROPERTY = "jdbc.output.field.count";

    /**
     * The name of the parameter to use for making Isolation level to be
     * read uncommitted by default for connections.
     */
    public static final String PROP_RELAXED_ISOLATION = "com.creditease.dbus.db.relaxedisolation";

    public static final String INPUT_SCHEMA_PROPERTY = "data.import.schema";

    public static final String DATA_IMPORT_CONSISTENT_READ_SCN = "data.import.consistent.read.scn";

    public static final String DATA_IMPORT_CONSISTENT_READ_SEQNO = "data.import.consistent.read.seqno";

    public static final String DATA_IMPORT_OP_TS = "data.import.op.ts";

    private String[] inputFieldNames;
//  /**
//   * Sets the DB access related fields in the {@link Properties}.
//   * @param conf the configuration
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param userName DB access username
//   * @param passwd DB access passwd
//   * @param fetchSize DB fetch size
//   * @param connectionParams JDBC connection parameters
//   */
//  public static void configureDB(Properties conf, String driverClass,
//      String dbUrl, String userName, String passwd, Integer fetchSize,
//      Properties connectionParams) {
//
//    conf.put(DRIVER_CLASS_PROPERTY, driverClass);
//    conf.put(URL_PROPERTY, dbUrl);
//    if (userName != null) {
//      conf.put(USERNAME_PROPERTY, userName);
//    }
////    if (passwd != null) {
////      setPassword((JobConf) conf, passwd);
////    }
//    if (fetchSize != null) {
//      conf.put(FETCH_SIZE, fetchSize);
//    }
//    if (connectionParams != null) {
//      conf.put(CONNECTION_PARAMS_PROPERTY,
//               propertiesToString(connectionParams));
//    }
//
//  }
//
//  // set the password in the secure credentials object
////  private static void setPassword(JobConf configuration, String password) {
////    LOG.debug("Securing password into job credentials store");
////    configuration.getCredentials().addSecretKey(
////      PASSWORD_SECRET_KEY, password.getBytes());
////  }
//
//  /**
//   * Sets the DB access related fields in the JobConf.
//   * @param job the job
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param fetchSize DB fetch size
//   * @param connectionParams JDBC connection parameters
//   */
//  public static void configureDB(Properties dbConf, String driverClass,
//      String dbUrl, Integer fetchSize, Properties connectionParams) {
//    configureDB(dbConf, driverClass, dbUrl, null, null, fetchSize,
//                connectionParams);
//  }
//
//  /**
//   * Sets the DB access related fields in the {@link Properties}.
//   * @param conf the configuration
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param userName DB access username
//   * @param passwd DB access passwd
//   * @param connectionParams JDBC connection parameters
//   */
//  public static void configureDB(Properties conf, String driverClass,
//      String dbUrl, String userName, String passwd,
//      Properties connectionParams) {
//    configureDB(conf, driverClass, dbUrl, userName, passwd, null,
//                connectionParams);
//  }
//
//  /**
//   * Sets the DB access related fields in the JobConf.
//   * @param job the job
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL.
//   * @param connectionParams JDBC connection parameters
//   */
//  public static void configureDB(Properties dbConf, String driverClass,
//      String dbUrl, Properties connectionParams) {
//    configureDB(dbConf, driverClass, dbUrl, null, connectionParams);
//  }
//
//  /**
//   * Sets the DB access related fields in the {@link Properties}.
//   * @param conf the configuration
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param userName DB access username
//   * @param passwd DB access passwd
//   * @param fetchSize DB fetch size
//   */
//  public static void configureDB(Properties conf, String driverClass,
//      String dbUrl, String userName, String passwd, Integer fetchSize) {
//    configureDB(conf, driverClass, dbUrl, userName, passwd, fetchSize,
//                (Properties) null);
//  }
//
//  /**
//   * Sets the DB access related fields in the JobConf.
//   * @param job the job
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param fetchSize DB fetch size
//   */
//  public static void configureDB(Properties job, String driverClass,
//      String dbUrl, Integer fetchSize) {
//    configureDB(job, driverClass, dbUrl, fetchSize, (Properties) null);
//  }
//
//  /**
//   * Sets the DB access related fields in the {@link Properties}.
//   * @param conf the configuration
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL
//   * @param userName DB access username
//   * @param passwd DB access passwd
//   */
//  public static void configureDB(Properties conf, String driverClass,
//      String dbUrl, String userName, String passwd) {
//    configureDB(conf, driverClass, dbUrl, userName, passwd, (Properties) null);
//  }
//
//  /**
//   * Sets the DB access related fields in the JobConf.
//   * @param job the job
//   * @param driverClass JDBC Driver class name
//   * @param dbUrl JDBC DB access URL.
//   */
//  public static void configureDB(Properties job, String driverClass,
//      String dbUrl) {
//    configureDB(job, driverClass, dbUrl, (Properties) null);
//  }


    private Properties properties;

    public DBConfiguration(Properties dbConf) {
        this.properties = dbConf;
    }

    // retrieve the password from the credentials object
//  public static String getPassword(JobConf configuration) {
//    LOG.debug("Fetching password from job credentials store");
//    byte[] secret = configuration.getCredentials().getSecretKey(
//      PASSWORD_SECRET_KEY);
//    return secret != null ? new String(secret) : null;
//  }

    public Properties getConfProperties() {
        return properties;
    }

    public Integer getPrepareStatementFetchSize() {
        int defaultFecthRecords = DataPullConstants.DEFAULT_PREPARE_STATEMENT_FETCH_RECORDS;
        try {
            Integer psFetchSizeConf = (Integer) (properties.get(DBConfiguration.PREPARE_STATEMENT_FETCH_SIZE));
            Integer dbRecordRowSize = (Integer) (properties.get(DBConfiguration.DB_RECORD_ROW_SIZE));
            LOG.info("psFetchSizeConf: {}, dbRecordRowSize: {}", psFetchSizeConf, dbRecordRowSize);
            int rows = psFetchSizeConf / dbRecordRowSize;
            if (rows == 0 || rows > defaultFecthRecords) {
                return defaultFecthRecords;
            }
            return rows;
        } catch (Exception e) {
            return defaultFecthRecords;
        }
    }

    public String getInputTableName() {
        //      String schema = (String)(properties.get(DBConfiguration.INPUT_SCHEMA_PROPERTY));
        //      String tableName = (String)(properties.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY));
        //
        //      return schema +"."+tableName;
        return (String) (properties.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY));
    }

//  public void setInputTableName(String tableName) {
//    conf.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
//  }

    public String[] getInputFieldNames() {
        return this.inputFieldNames;
    }

    public void setInputFieldNames(String[] inputFieldNames) {
        this.inputFieldNames = inputFieldNames;
    }

    public String getInputConditions() {
        return (String) (properties.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY));
    }

    public String getInputOrderBy() {
        return (String) (properties.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY));
    }

    public void setInputOrderBy(String orderby) {
        if (orderby != null && orderby.length() > 0) {
            properties.put(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
        }
    }

    public String getInputQuery() {
        return (String) (properties.get(DBConfiguration.INPUT_QUERY));
    }

    public void setInputQuery(String query) {
        if (query != null && query.length() > 0) {
            properties.put(DBConfiguration.INPUT_QUERY, query);
        }
    }

    public String getInputCountQuery() {
        return (String) (properties.get(DBConfiguration.INPUT_COUNT_QUERY));
    }

    public long getInputSplitMappersNum() {
        return properties.get(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS) == null ? 1L : (Long) (properties.get(DBConfiguration.INPUT_SPLIT_NUM_MAPPERS));
    }

    public long getInputSplitLimit() {
        return properties.get(DBConfiguration.INPUT_SPLIT_LIMIT) == null ? -1 : (Integer) (properties.get(DBConfiguration.INPUT_COUNT_QUERY));
    }

    public int getSplitShardSize() {
        return properties.get(DBConfiguration.SPLIT_SHARD_SIZE) == null
                ? DataPullConstants.DEFAULT_SPLIT_SHARD_SIZE
                : (Integer) properties.get(DBConfiguration.SPLIT_SHARD_SIZE);
    }

    public boolean getAllowTextSplitter() {
//        boolean allowTextSplitter = false;
//        try {
//            allowTextSplitter = (Boolean) (properties.get(DBConfiguration.INPUT_SPLIT_LIMIT));
//        }
//        catch (Exception e) {
//            // Just skip
//        }
//        return allowTextSplitter;
        return true;
    }

    public Object get(String propKey) {
        return properties.get(propKey);
    }

    public int getInt(String propKey, int defaultValue) {
        try {
            Object prop = properties.get(propKey);
            return prop == null ? defaultValue : (Integer) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public Boolean getBoolean(String propKey, boolean defaultValue) {
        try {
            Object prop = properties.get(propKey);
            return prop == null ? defaultValue : (Boolean) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public String getString(String propKey) {
        return getString(propKey, null);
    }

    public String getString(String propKey, String defaultStr) {
        return properties.get(propKey) != null ? (String) properties.get(propKey) : defaultStr;
    }

    public Long getLong(String propKey, Long defaultValue) {
        try {
            Object prop = properties.get(propKey);
            return prop == null ? defaultValue : (Long) prop;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public void set(String propKey, Object value) {
        properties.put(propKey, value);
    }
//  public void setInputCountQuery(String query) {
//    if (query != null && query.length() > 0) {
//      conf.put(DBConfiguration.INPUT_COUNT_QUERY, query);
//    }
//  }

    public void setInputBoundingQuery(String query) {
        if (query != null && query.length() > 0) {
            properties.put(DBConfiguration.INPUT_BOUNDING_QUERY, query);
        }
    }

    public String getInputBoundingQuery() {
        return properties.get(DBConfiguration.INPUT_BOUNDING_QUERY) == null
                ? null
                : (String) (properties.get(DBConfiguration.INPUT_BOUNDING_QUERY));
    }

    /**
     * Converts connection properties to a String to be passed to the mappers.
     * @param properties JDBC connection parameters
     * @return String to be passed to configuration
     */
    protected static String propertiesToString(Properties properties) {
        List<String> propertiesList = new ArrayList<String>(properties.size());
        for (Entry<Object, Object> property : properties.entrySet()) {
            String key = StringEscapeUtils.escapeCsv(property.getKey().toString());
            if (key.equals(property.getKey().toString()) && key.contains("=")) {
                key = "\"" + key + "\"";
            }
            String val = StringEscapeUtils.escapeCsv(property.getValue().toString());
            if (val.equals(property.getValue().toString()) && val.contains("=")) {
                val = "\"" + val + "\"";
            }
            propertiesList.add(StringEscapeUtils.escapeCsv(key + "=" + val));
        }
        return StringUtils.join(propertiesList, ',');
    }

    /**
     * Converts a String back to connection parameters.
     * @param input String from configuration
     * @return JDBC connection parameters
     */
    public static Properties propertiesFromString(String input) {
        if (input != null && !input.isEmpty()) {
            Properties result = new Properties();
            StrTokenizer propertyTokenizer = StrTokenizer.getCSVInstance(input);
            StrTokenizer valueTokenizer = StrTokenizer.getCSVInstance();
            valueTokenizer.setDelimiterChar('=');
            while (propertyTokenizer.hasNext()) {
                valueTokenizer.reset(propertyTokenizer.nextToken());
                String[] values = valueTokenizer.getTokenArray();
                if (values.length == 2) {
                    result.put(values[0], values[1]);
                }
            }
            return result;
        } else {
            return null;
        }
    }

    public boolean isDirect() {
        return getBoolean(DIRECT_IMPORT, false);
    }


//  public Long getScn(){
//    Object consistentReadScnObj = conf.get(DataPullConstants.ORAOOP_IMPORT_CONSISTENT_READ_SCN);
//  
//      if(consistentReadScnObj==null){
//          try {
//            consistentReadScnObj=SqlManager.getCurrentScn(getConnection());
//            conf.put(DataPullConstants.ORAOOP_IMPORT_CONSISTENT_READ_SCN, consistentReadScnObj);
//        }
//        catch (ClassNotFoundException e) {
//            // TODO Auto-generated catch block
//            LOG.error(e.getMessage(),e);
//        }
//        catch (SQLException e) {
//            // TODO Auto-generated catch block
//            LOG.error(e.getMessage(),e);
//        }
//      }
//     
//      return consistentReadScnObj==null?0L:(Long)consistentReadScnObj;
//  }

    public String getDbTypeAndNameSpace(String dataSourceInfo, String seriesTableName, String tablePartition) {
        String dbType = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DS_TYPE));
        String dbName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
        JSONObject ds = JSONObject.parseObject(dataSourceInfo);
        JSONObject payload = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
        String dbSchema = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
        String version = payload.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
        // 约定格式：database.db.table.v2.dbpar01.tablepar01-----20180418废弃
        // return String.format("%s.%s.%s.%s.%s.%s.%s", dbType, dbName, dbSchema, tableName, version, "0", tablePartition);
        /*
         * 20180418约定:partition没啥用。可不考虑。
         * 系列表中每个分表的表名却有了现实的需求。
         * 所以，partition的位置放系列表名。
         * 约定格式：database.db.table.v2.dbpar.seriesTableName
         * 非系列表的情况，置为 “0”
         * */
        if (seriesTableName.indexOf(".") != -1) {
            seriesTableName = seriesTableName.split("\\.")[1];// 传入参数可能是schema.seriesTableName形式。需要剔除schema信息
        }
        if (seriesTableName.equals(tableName)) {
            // 只有系列表，才需要将系列表名放置到namespace末级。
            // 如果seriesTableName和输出表名一致，表明是本表，而非系列表，partition部分需置为"0"。
            seriesTableName = "0";
        }

        if (FullPullHelper.isDbusKeeper(dataSourceInfo)) {
            String topoName = FullPullHelper.getTopoName(dataSourceInfo);
            return String.format("%s.%s!%s.%s.%s.%s.%s.%s", dbType, dbName, topoName, dbSchema, tableName, version, "0", seriesTableName);
        } else {
            return String.format("%s.%s.%s.%s.%s.%s.%s", dbType, dbName, dbSchema, tableName, version, "0", seriesTableName);
        }
    }

    public String buildSlashedNameSpace(String dataSourceInfo, String outputVersion) {
        String dbName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
        JSONObject ds = JSONObject.parseObject(dataSourceInfo);

        //String timestamp = ds.getString(DataPullConstants.FullPullInterfaceJson.TIMESTAMP_KEY);
        //timestamp = timestamp.replace(":", ".");
        Long id = ds.getLong(DataPullConstants.FullPullInterfaceJson.ID_KEY);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
        String timestamp = sdf.format(new Date(id));
        JSONObject payload = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
        String dbSchema = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);

        if (outputVersion.equals(Constants.VERSION_12)) {
            String versionNo = payload.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
            return String.format("%s/%s/%s/%s", dbName, dbSchema, tableName, versionNo);
        } else {
            String batchNo = payload.getString(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY);
            return String.format("%s/%s/%s/%s - %s", dbName, dbSchema, tableName, timestamp, batchNo);
        }

    }

    public String buildNameSpaceForZkUidFetch(String dataSourceInfo) {
        String dbName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
        return String.format("%s.schema.table.version", dbName);

    }

//  public String buildDottedNameSpace(String dataSourceInfo){
//      String dbName = (String)(this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
//      JSONObject ds=JSONObject.parseObject(dataSourceInfo);
//      JSONObject payload = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
//      //      String dbSchema = (String)(this.properties.get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
//      //      String tableName = (String)(this.properties.get(DBConfiguration.DataSourceInfo.TABLE_NAME));
//      String dbSchema = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
//      String tableName = payload.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
//      String version = payload.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
//      return String.format("%s.%s.%s.%s", dbName, dbSchema, tableName, version);
//
//  }

    public String getDbNameAndSchema() {
        String dbName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
        String dbSchema = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
        return dbName + "/" + dbSchema;
    }

    public String getKafkaKey(String dataSourceInfo, String tablePartition) {
        String dbType = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DS_TYPE));
        String dbName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_NAME));
        String dbSchema = (String) (this.properties.get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
        String tableName = (String) (this.properties.get(DBConfiguration.DataSourceInfo.TABLE_NAME));

        JSONObject ds = JSONObject.parseObject(dataSourceInfo);
        JSONObject payload = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
        String version = payload.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
        // 应EDP要求，kafka key格式调整为：类型+ namespace+ dbus占位符 + wh_占位符
        // 例子：data_initial_data.mysql.db1.schema1.table1.5.0.0.1481245701166.wh_placeholder
        // dbus占位符目前我们只需要放timestamp。wh_placeholder是预留备用。目前EDP team还没用。由于这个占位符只有这里用到，暂时定义常量。
        if (FullPullHelper.isDbusKeeper(dataSourceInfo)) {
            String topoName = FullPullHelper.getTopoName(dataSourceInfo);
            return String.format("%s.%s.%s!%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    topoName, dbSchema, tableName, version, '0', tablePartition, System.currentTimeMillis(), "wh_placeholder");
        } else {
            return String.format("%s.%s.%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    dbSchema, tableName, version, '0', tablePartition, System.currentTimeMillis(), "wh_placeholder");
        }
    }


    // 同一份缓存的dbConf可能应对多个版本的拉取。版本需从即时dataSourceInfo获取
//  private String getVersionByDataSourceInfo(String dataSourceInfo) {
//     return String.valueOf((JSONObject.parseObject(dataSourceInfo).getJSONObject("payload").get(DataPullConstants.FullPullInterfaceJson.VERSION_KEY)));
//  }
}
