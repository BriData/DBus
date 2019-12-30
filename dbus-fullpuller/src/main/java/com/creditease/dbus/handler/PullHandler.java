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


package com.creditease.dbus.handler;


import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.msgencoder.EncodeColumn;
import com.creditease.dbus.msgencoder.PluggableMessageEncoder;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.msgencoder.UmsEncoder;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PullHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public OutputCollector collector;
    //每次发送给kafka的数据最大值
    public Long kafkaSendBatchSize;
    //每次发送给hdfs的数据最大值
    public Long hdfsSendBatchSize;
    //每次发给kafka的行数，与kafkaSendBatchSize配合使用，谁先满足条件，谁就生效
    public Long kafkaSendRows;
    //hdfs文件最大值128M
    public Long hdfsFileMaxSize;
    public Producer stringProducer;

    public FileSystem fileSystem;

    public abstract void doPullingProcess(Tuple input, String reqString, InputSplit inputSplit, Long splitIndex) throws Exception;

    public int getEmptyUMSlength(String outputVersion, String reqString, DBConfiguration dbConf,
                                 ResultSetMetaData rsmd, MetaWrapper metaInDbus, String seriesTableName, int batchNo) throws Exception {
        DbusMessageBuilder builder = new DbusMessageBuilder(outputVersion);
        builder.build(DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbConf.getDbTypeAndNameSpace(reqString, seriesTableName), batchNo);
        String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            MetaWrapper.MetaCell metaCell = metaInDbus.get(columnName);
            if (metaCell == null) {
                logger.error("get meta from dbus is null . columnName:{} ", columnName);
                throw new RuntimeException("Detected that META is not compatible now ! meta from dbus is null.");
            }
            Integer dataPrecision = metaCell.getDataPrecision();
            Integer dataScale = metaCell.getDataScale();
            String dataTypeMgr = metaCell.getDataType();
            boolean nullable = metaCell.isNullable();
            DataType dataType = DataType.convertDataType(dsType, dataTypeMgr, dataPrecision, dataScale);
            builder.appendSchema(columnName, dataType, nullable);
        }

        return builder.getMessage().toString().getBytes().length;
    }

    public DbusMessage buildResultMessage(String outputVersion, List<List<Object>> tuples, String reqString, DBConfiguration dbConf,
                                          ResultSetMetaData rsmd, MetaWrapper metaInDbus, String seriesTableName, int batchNo) throws Exception {

        DbusMessageBuilder builder = new DbusMessageBuilder(outputVersion);
        builder.build(DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbConf.getDbTypeAndNameSpace(reqString, seriesTableName), batchNo);
        String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            MetaWrapper.MetaCell metaCell = metaInDbus.get(columnName);
            if (metaCell == null) {
                logger.error("get meta from dbus is null . columnName:{} ", columnName);
                throw new RuntimeException("Detected that META is not compatible now ! meta from dbus is null.");
            }
            Integer dataPrecision = metaCell.getDataPrecision();
            Integer dataScale = metaCell.getDataScale();
            String dataTypeMgr = metaCell.getDataType();
            boolean nullable = metaCell.isNullable();
            DataType dataType = DataType.convertDataType(dsType, dataTypeMgr, dataPrecision, dataScale);
            builder.appendSchema(columnName, dataType, nullable);
        }

        for (List<Object> tuple : tuples) {
            builder.appendPayload(tuple.toArray());
        }

        DbusMessage message = builder.getMessage();

        //****************数据脱敏开始********************
        UmsEncoder encoder = new PluggableMessageEncoder(PluginManagerProvider.getManager(), (e, column, m) -> {
            // TODO: 2018/5/25
        });
        //去除因为表结构表更,删除列,rename列,可能导致的脱敏报错
        List<String> outputFields = Arrays.asList(dbConf.getInputFieldNames());
        List<EncodeColumn> encodeColumns = (List<EncodeColumn>) dbConf.get(DBConfiguration.TABEL_ENCODE_COLUMNS);
        Iterator<EncodeColumn> iterator = encodeColumns.iterator();
        while (iterator.hasNext()) {
            EncodeColumn next = iterator.next();
            boolean flag = false;
            for (String outputField : outputFields) {
                if (outputField.indexOf("`") != -1) {
                    outputField = outputField.substring(1, outputField.length() - 1);
                }
                if (outputField.equalsIgnoreCase(next.getFieldName())) {
                    flag = true;
                }
            }
            if (!flag) iterator.remove();
        }
        encoder.encode(message, (List<EncodeColumn>) dbConf.get(DBConfiguration.TABEL_ENCODE_COLUMNS));
        //****************数据脱敏结束********************
        return message;
    }

    public void sendMessageToKafka(String resultTopic, String key, DbusMessage dbusMessage,
                                   AtomicLong sendCnt, AtomicLong recvCnt, AtomicBoolean isError) throws Exception {
        if (stringProducer == null) {
            throw new Exception("producer is null, can't send to kafka!");
        }

        ProducerRecord record = new ProducerRecord<>(resultTopic, key, dbusMessage.toString());
        sendCnt.getAndIncrement();
        stringProducer.send(record, (metadata, e) -> {
            if (e != null) {
                logger.error("Send Message to kafka exception!", e);
                isError.set(true);
            } else {
                recvCnt.getAndIncrement();
            }
        });
    }

    /**
     * 强制重新获取新的ZkService
     * 这样保证每次拉全量的UMS_UID都重新从ZK上获取
     * 避免比增量当前UMS_UID小
     * 以保持UMS_UID的有序递增（不要求连续）
     */
    public ZkService reloadZkService() throws Exception {
        ZkService zkService = null;
        try {
            Properties commonProps = FullPullHelper.getFullPullProperties(FullPullConstants.COMMON_CONFIG, true);
            String zkConString = commonProps.getProperty(FullPullConstants.FULL_PULL_MONITORING_ZK_CONN_STR);
            zkService = new ZkService(zkConString);
        } catch (Exception e) {
            logger.error("Create new zkservice failed. Exception:" + e);
            throw new Exception("Create new zkservice failed." + e.getMessage(), e);
        }
        if (zkService == null) {
            logger.error("zkservice is null ");
            throw new Exception("reload zkservice failed.");
        }
        return zkService;
    }

    public boolean isKafkaSend(long totalMemSize, long totalRows) {
        if (totalMemSize >= kafkaSendBatchSize || totalRows >= kafkaSendRows) {
            return true;
        }
        return false;
    }

    public void emitMonitorState(Tuple input, String reqString, String monitorNodePath, long finishedRow, long finishedShardCount) {
        JSONObject jsonInfo = new JSONObject();
        jsonInfo.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);
        jsonInfo.put(FullPullConstants.DATA_MONITOR_ZK_PATH, monitorNodePath);
        jsonInfo.put(FullPullConstants.DB_NAMESPACE_NODE_FINISHED_COUNT, finishedShardCount);
        jsonInfo.put(FullPullConstants.DB_NAMESPACE_NODE_FINISHED_ROWS, finishedRow);
        collector.emit(input, new Values(jsonInfo));
    }

    public boolean isSendFinished(AtomicLong send, AtomicLong recv) {
        long startTime = System.currentTimeMillis();
        while ((send.get() > recv.get())) {
            TimeUtils.sleep(1000);
            //等待1分钟仍然没有收到recv就认为写入失败
            if (System.currentTimeMillis() - startTime > 60000) {
                logger.error("System.currentTimeMillis()-startTime  is {}", (System.currentTimeMillis() - startTime));
                return false;
            }
        }
        return true;
    }

    public void setCollector(OutputCollector collector) {
        this.collector = collector;
    }

    public void setKafkaSendBatchSize(Long kafkaSendBatchSize) {
        this.kafkaSendBatchSize = kafkaSendBatchSize;
    }

    public void setKafkaSendRows(Long kafkaSendRows) {
        this.kafkaSendRows = kafkaSendRows;
    }

    public void setHdfsSendBatchSize(Long hdfsSendBatchSize) {
        this.hdfsSendBatchSize = hdfsSendBatchSize;
    }

    public void setHdfsFileMaxSize(Long hdfsFileMaxSize) {
        this.hdfsFileMaxSize = hdfsFileMaxSize;
    }

    public void setStringProducer(Producer stringProducer) {
        this.stringProducer = stringProducer;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
}
