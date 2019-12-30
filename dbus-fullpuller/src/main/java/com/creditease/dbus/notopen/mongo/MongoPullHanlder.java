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


package com.creditease.dbus.notopen.mongo;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.MultiTenancyHelper;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.handler.PullHandler;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.msgencoder.EncodeColumn;
import com.creditease.dbus.msgencoder.PluggableMessageEncoder;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.msgencoder.UmsEncoder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/10
 */
public class MongoPullHanlder extends PullHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void doPullingProcess(Tuple input, String reqString, InputSplit inputSplit, Long splitIndex) throws Exception {
        fetchingData(input, reqString, inputSplit, splitIndex);
    }

    private void fetchingData(Tuple input, String reqString, InputSplit inputSplit, Long splitIndex) throws Exception {
        MongoManager mongoManager = null;
        Long startTime = System.currentTimeMillis();
        ZkService localZkService = null;
        long dealRowCnt = 0;
        long dealRowMemSize = 0;
        long sendRowsCnt = 0;
        AtomicLong sendCnt = new AtomicLong(0);
        AtomicLong recvCnt = new AtomicLong(0);
        AtomicBoolean isError = new AtomicBoolean(false);
        try {
            JSONObject reqJson = JSONObject.parseObject(reqString);
            JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
            int batchNo = payloadJson.getIntValue(FullPullConstants.REQ_PAYLOAD_BATCH_NO);

            DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);

            String dsKey = FullPullHelper.getDataSourceKey(reqJson);


            long lastUpdatedMonitorTime = System.currentTimeMillis();

            long monitorTimeInterval = FullPullConstants.HEARTBEAT_MONITOR_TIME_INTERVAL_DEFAULT_VAL;
            Properties commonProps = FullPullHelper.getFullPullProperties(FullPullConstants.COMMON_CONFIG, true);
            String monitorTimeIntervalConf = commonProps.getProperty(FullPullConstants.HEARTBEAT_MONITOR_TIME_INTERVAL);
            if (monitorTimeIntervalConf != null) {
                monitorTimeInterval = Long.valueOf(monitorTimeIntervalConf);
            }

            MongoInputSplit mongoInputSplit = (MongoInputSplit) inputSplit;

            String mongoUrl = mongoInputSplit.getPullTargetMongoUrl();
            String username = dbConf.getString(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY);
            String password = dbConf.getString(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY);
            mongoManager = new MongoManager(mongoUrl, username, password);

            String[] tableNameWithSchema = dbConf.getInputTableName().split("\\.");
            String schema = tableNameWithSchema[0];
            String tableName = tableNameWithSchema[1];

            String monitorNodePath = FullPullHelper.getMonitorNodePath(reqString);
            String opTs = dbConf.getString(DBConfiguration.DATA_IMPORT_OP_TS);
            localZkService = reloadZkService();
            String resultKey = getKafkaKey(reqString, dbConf);
            String outputVersion = FullPullHelper.getOutputVersion();
            String resultTopic = (String) dbConf.get(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC);

            Document dataQueryObject = mongoInputSplit.getDataQueryObject();
            FindIterable<Document> documents = mongoManager.find(schema, tableName, dataQueryObject);
            MongoCursor<Document> iterator = documents.iterator();
            //一级展开
            boolean openFirst = MongoHelper.getMongoOpenFirst(reqJson);
            Document document = null;
            List<List<Object>> tuples = new ArrayList<>();
            JSONObject jsonObj = new JSONObject();
            while (iterator.hasNext()) {
                document = iterator.next();
                List<Object> rowDataValues = new ArrayList<>();
                String pos = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_POS); // TODO
                rowDataValues.add(pos); // TODO
                rowDataValues.add(opTs);
                rowDataValues.add("i");
                long uniqId = localZkService.nextValue(dbConf.buildNameSpaceForZkUidFetch(reqString));
                rowDataValues.add(String.valueOf(uniqId)); // 全局唯一 _ums_uid_
                dealRowCnt++;
                if (openFirst) {
                    Set<Map.Entry<String, Object>> entries = document.entrySet();
                    for (Map.Entry<String, Object> entry : entries) {
                        rowDataValues.add(entry.getValue());
                    }
                    tuples.add(rowDataValues);
                    DbusMessage dbusMessage = buildMongoResultMessage(outputVersion, reqString, dbConf, batchNo, tuples, document, openFirst);
                    //将数据写入kafka
                    sendMessageToKafka(resultTopic, resultKey, dbusMessage, sendCnt, recvCnt, isError);
                    tuples.clear();

                    if (isError.get()) {
                        throw new Exception("kafka send exception!");
                    }
                    long updatedMonitorInterval = (System.currentTimeMillis() - lastUpdatedMonitorTime) / 1000;
                    if (updatedMonitorInterval > monitorTimeInterval) {
                        //1. 隔一段时间刷新monitor zk状态,最后一个参数：一片尚未完成，计数0
                        emitMonitorState(input, reqString, monitorNodePath, dealRowCnt, 0);
                        long endTime = (System.currentTimeMillis() - startTime) / 1000;
                        logger.info("[pull bolt] dsKey: {}, pull_index{}: finished {} rows. consume time {}s", dsKey, splitIndex, dealRowCnt, endTime);
                        lastUpdatedMonitorTime = System.currentTimeMillis();
                        dealRowCnt = 0;

                        //2. 如果已经出现错误，跳过后来的tuple数据
                        ProgressInfo progressInfo = FullPullHelper.getMonitorInfoFromZk(localZkService, monitorNodePath);
                        if (progressInfo.getErrorMsg() != null) {
                            logger.error("Get progressInfo failed，skipped index:" + splitIndex);
                            collector.fail(input);
                            return;
                        }
                    }
                } else {
                    sendRowsCnt++;

                    dealRowMemSize += String.valueOf(document).getBytes().length;
                    Set<Map.Entry<String, Object>> entries = document.entrySet();
                    for (Map.Entry<String, Object> entry : entries) {
                        jsonObj.put(entry.getKey(), entry.getValue());
                    }

                    rowDataValues.add(jsonObj.toJSONString());
                    tuples.add(rowDataValues);
                    if (isKafkaSend(dealRowMemSize, sendRowsCnt)) {
                        dealRowMemSize = 0;
                        sendRowsCnt = 0;
                        DbusMessage dbusMessage = buildMongoResultMessage(outputVersion, reqString, dbConf, batchNo, tuples, document, openFirst);
                        //将数据写入kafka
                        sendMessageToKafka(resultTopic, resultKey, dbusMessage, sendCnt, recvCnt, isError);
                        tuples.clear();
                        jsonObj.clear();
                    }

                    if (isError.get()) {
                        throw new Exception("kafka send exception!");
                    }
                    long updatedMonitorInterval = (System.currentTimeMillis() - lastUpdatedMonitorTime) / 1000;
                    if (updatedMonitorInterval > monitorTimeInterval) {
                        //1. 隔一段时间刷新monitor zk状态,最后一个参数：一片尚未完成，计数0
                        emitMonitorState(input, reqString, monitorNodePath, dealRowCnt, 0);
                        long endTime = (System.currentTimeMillis() - startTime) / 1000;
                        logger.info("[pull bolt] dsKey: {}, pull_index{}: finished {} rows. consume time {}s", dsKey, splitIndex, dealRowCnt, endTime);
                        lastUpdatedMonitorTime = System.currentTimeMillis();
                        dealRowCnt = 0;

                        //2. 如果已经出现错误，跳过后来的tuple数据
                        ProgressInfo progressInfo = FullPullHelper.getMonitorInfoFromZk(localZkService, monitorNodePath);
                        if (progressInfo.getErrorMsg() != null) {
                            logger.error("Get progressInfo failed，skipped index:" + splitIndex);
                            collector.fail(input);
                            return;
                        }
                    }
                }

            }
            if (tuples.size() > 0) {
                DbusMessage dbusMessage = buildMongoResultMessage(outputVersion, reqString, dbConf, batchNo, tuples, document, openFirst);
                //发送剩余的数据到 result topic
                sendMessageToKafka(resultTopic, resultKey, dbusMessage, sendCnt, recvCnt, isError);
            }
            boolean isFinished = isSendFinished(sendCnt, recvCnt);
            if (isFinished) {
                //最后一个参数：完成一片，计数1
                emitMonitorState(input, reqString, monitorNodePath, dealRowCnt, 1);
                long endTime = (System.currentTimeMillis() - startTime) / 1000;
                logger.info("[pull bolt] dsKey: {}, pull_index{}: finished {} rows. consume time {}s", dsKey, splitIndex, dealRowCnt, endTime);
                collector.ack(input);
            } else {
                throw new Exception("Waiting kafka ack timeout!");
            }

            logger.info("[pull bolt] dsKey: {}, pull_index{} pull finished.  total {} rows", dsKey, splitIndex, dealRowCnt);
        } catch (Exception e) {
            logger.error("Exception happened when fetching data.", e);
            throw e;
        } finally {
            if (mongoManager != null) {
                mongoManager.close();
            }
            if (localZkService != null) {
                localZkService.close();
            }
        }
    }

    private DbusMessage buildMongoResultMessage(String outputVersion, String reqString,
                                                DBConfiguration dbConf, int batchNo, List<List<Object>> tuples,
                                                Document document, Boolean openFirst) throws Exception {
        String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        DbusMessageBuilder builder = new DbusMessageBuilder(outputVersion);
        builder.build(DbusMessage.ProtocolType.DATA_INITIAL_DATA, getDbTypeAndNameSpace(reqString, dbConf), batchNo);
        if (openFirst) {
            Set<Map.Entry<String, Object>> entries = document.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                DataType dataType = DataType.convertDataType(dsType, coverToJavaDataType(entry.getValue()), null, null);
                builder.appendSchema(entry.getKey(), dataType, false);
            }
        } else {
            builder.appendSchema("jsonObj", DataType.JSONOBJECT, false);
        }
        for (List<Object> tuple : tuples) {
            builder.appendPayload(tuple.toArray());
        }

        DbusMessage message = builder.getMessage();
        // 脱敏
        UmsEncoder encoder = new PluggableMessageEncoder(PluginManagerProvider.getManager(), (e, column, m) -> {
            // TODO: 2018/5/25
        });
        encoder.encode(message, (List<EncodeColumn>) dbConf.get(DBConfiguration.TABEL_ENCODE_COLUMNS));
        return message;
    }

    private String coverToJavaDataType(Object value) {
        if (value instanceof String) return "STRING";
        if (value instanceof Boolean) return "BOOLEAN";
        if (value instanceof Double) return "DOUBLE";
        if (value instanceof Date) return "DATE";
        if (value instanceof Integer) return "INTEGER";
        if (value instanceof Long) return "LONG";
        if (value instanceof ObjectId) return "OBJECTID";
        return "";
    }

    public String getDbTypeAndNameSpace(String reqString, DBConfiguration dbConf) throws Exception {
        String dbType = (String) dbConf.get(DBConfiguration.DataSourceInfo.DS_TYPE);
        String dbName = (String) dbConf.get(DBConfiguration.DataSourceInfo.DB_NAME);
        JSONObject reqJson = JSONObject.parseObject(reqString);
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        String dbSchema = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME);
        String tableName = payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME);

        if (MultiTenancyHelper.isMultiTenancy(reqString)) {
            String topoName = (String) (dbConf.get(FullPullConstants.REQ_PROJECT_TOPO_NAME));
            return String.format("%s.%s!%s.%s.%s.*.0.0", dbType, dbName, topoName, dbSchema, tableName);
        } else {
            return String.format("%s.%s.%s.%s.*.0.0", dbType, dbName, dbSchema, tableName);
        }
    }

    public String getKafkaKey(String reqString, DBConfiguration dbConf) throws Exception {
        String dbType = (String) dbConf.get(DBConfiguration.DataSourceInfo.DS_TYPE);
        String dbName = (String) dbConf.get(DBConfiguration.DataSourceInfo.DB_NAME);
        String dbSchema = (String) dbConf.get(DBConfiguration.DataSourceInfo.DB_SCHEMA);
        String tableName = (String) dbConf.get(DBConfiguration.DataSourceInfo.TABLE_NAME);

        if (MultiTenancyHelper.isMultiTenancy(reqString)) {
            String topoName = (String) (dbConf.get(FullPullConstants.REQ_PROJECT_TOPO_NAME));
            return String.format("%s.%s.%s!%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    topoName, dbSchema, tableName, "*", "0", "0", System.currentTimeMillis(), "wh_placeholder");
        } else {
            return String.format("%s.%s.%s.%s.%s.%s.%s.%s.%s.%s", DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbType, dbName,
                    dbSchema, tableName, "*", "0", "0", System.currentTimeMillis(), "wh_placeholder");
        }
    }

}
