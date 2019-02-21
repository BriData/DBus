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

package com.creditease.dbus.bolt;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.*;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DBRecordReader;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.GenericJdbcManager;
import com.creditease.dbus.msgencoder.EncodeColumn;
import com.creditease.dbus.msgencoder.PluggableMessageEncoder;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.msgencoder.UmsEncoder;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class PagedBatchDataFetchingBolt extends BaseRichBolt {
    private Logger LOG = LoggerFactory.getLogger(getClass());
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private String topologyId;
    private String zkconnect;
    private String dsName;
    private String zkTopoRoot;
    ZkService zkService = null;
    private AtomicLong kafkaSendBatchSize = new AtomicLong(1000000); // 每次发送给kafka的数据量 1M
    private AtomicLong kafkaSendRows = new AtomicLong(1000); //每次发给kafka的行数，与kafkaSendBatchSize配合使用，谁先满足条件，谁就生效

    private Properties commonProps;
    private Properties stringProducerProps;
    private Producer stringProducer;
    private Map confMap;
    private String resultTopic;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.topologyId = (String) conf.get(Constants.StormConfigKey.FULL_PULLER_TOPOLOGY_ID);
        this.zkconnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
        this.zkTopoRoot = Constants.TOPOLOGY_ROOT + "/" + Constants.FULL_PULLING_PROPS_ROOT;

        //设置topo类型，用于获取配置信息路径
        FullPullHelper.setTopologyType(Constants.FULL_PULLER_TYPE);

        loadRunningConf(null);

        LOG.info("PagedBatchDataFetchingBolt init OK!");
    }

    public void execute(Tuple input) {
        String dsKey = null;
        JSONObject jsonObject = null;
        String dataSourceInfo = null;
        String splitIndex = null;
        ResultSet rs = null;
        GenericJdbcManager dbManager = null;
        DBRecordReader dbRecordReader = null;
        String msg = (String) input.getValue(0);
        try {
            jsonObject = JSONObject.parseObject(msg);
            dataSourceInfo = jsonObject.getString(DataPullConstants.DATA_SOURCE_INFO);
            JSONObject dsObject = JSONObject.parseObject(dataSourceInfo);
            JSONObject payloadObject = dsObject.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
            int batchNo = payloadObject.getIntValue(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY);
            String cmdType = dsObject.getString("type");
            if (null == cmdType) {
                LOG.error("the type of request is null on PagedBatchDataFetchingBolt!");
                collector.fail(input);
                return;
            } else if (cmdType.equals(DataPullConstants.COMMAND_FULL_PULL_STOP)) {
                LOG.error("Impossible to be here!!! the type of request is COMMAND_FULL_PULL_STOP on PagedBatchDataFetchingBolt!");
                return;
            } else if (cmdType.equals(DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                //处理reload事件
                loadRunningConf(dataSourceInfo);

                // 将load conf请求传导到下级bolt
                collector.emit(new Values(jsonObject));
                // 不跟踪消息的处理, 也不需要ack

                LOG.info("receive RELOAD command OK!");
                return;
            }

            /**
             * 强制重新获取新的ZkService
             * 这样保证每次拉全量的UMS_UID都重新从ZK上获取
             * 避免比增量当前UMS_UID小
             * 以保持UMS_UID的有序递增（不要求连续）
             */

            ZkService localZkService = reloadZkServiceRunningConf();
            if (localZkService == null) {
                LOG.error("generate new zkservice failed for ums_uid, stop pulling");
                return;
            }

            //以下都是数据事件
            JSONObject dataSplitShard = jsonObject;

            //出错提前退出逻辑，如果取不到progress信息或者已经出现错误，跳过后来的tuple数据
            String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
            ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
            String status = FullPullHelper.getTaskStateByHistoryTable(dsObject);
            if (progressObj.getErrorMsg() != null || (StringUtils.isNotBlank(status) && status.equalsIgnoreCase("abort"))) {
                splitIndex = dataSplitShard.getString(DataPullConstants.DATA_CHUNK_SPLIT_INDEX);
                LOG.error("Get process failed，skipped index:" + splitIndex);
                collector.fail(input);
                return;
            }

            splitIndex = dataSplitShard.getString(DataPullConstants.DATA_CHUNK_SPLIT_INDEX);
            String totalRows = dataSplitShard.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_TOTAL_ROWS);
            String startSecs = dataSplitShard.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_START_SECS);
            String totalPartitions = dataSplitShard.getString(DataPullConstants.DATA_CHUNK_COUNT);

            JSONObject inputSplitJsonObject = dataSplitShard.getJSONObject(DataPullConstants.DATA_CHUNK_SPLIT);
            DataDrivenDBInputFormat.DataDrivenDBInputSplit inputSplit = inputSplitJsonObject
                    .toJavaObject(DataDrivenDBInputFormat.DataDrivenDBInputSplit.class);
            String tablePartition = inputSplit.getTablePartitionInfo();
            tablePartition = StringUtils.isNotBlank(tablePartition) ? tablePartition : "0";

            DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
            String logicalTableName = dbConf.getInputTableName();
            String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            resultTopic = (String) dbConf.get(DBConfiguration.DataSourceInfo.OUTPUT_TOPIC);

//            String targetTableName = inputSplit.getTargetTableName();
//
//            //Oracle的情况，patition 默认设为0 .Mysql,有分表的，为分表名。无分表的，为0。
//            String splittedTableInfo = "0";
//            //targetTableName = "sub.trade"
//            if(!targetTableName.equalsIgnoreCase(logicalTableName)) { //目标表名和逻辑表名不一致，表示是分表。
//                splittedTableInfo = targetTableName.indexOf(".") == -1
//                        ? targetTableName
//                        : targetTableName.split("\\.")[1];//目标表名可能带schema。partion内容是不带schema的分表名
//            }

            String resultKey = dbConf.getKafkaKey(dataSourceInfo, tablePartition);
            dsKey = FullPullHelper.getDataSourceKey(dsObject);
            dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
            String opTs = dbConf.getString(DBConfiguration.DATA_IMPORT_OP_TS);

            AtomicLong sendCnt = new AtomicLong(0);
            AtomicLong recvCnt = new AtomicLong(0);
            AtomicBoolean isError = new AtomicBoolean(false);
            long startTime = System.currentTimeMillis();

            dbRecordReader = DBHelper.getRecordReader(dbManager, dbConf, inputSplit, logicalTableName);
            //oracle表的meta信息单独获取
            //从dbus manager库中获取meta信息
            MetaWrapper metaInDbus = DBHelper.getMetaInDbus(dataSourceInfo);
            //HashMap<String, HashMap<String, Object>> metaData = null;
            //if (datasourceType.toUpperCase().equals(DbusDatasourceType.ORACLE.name())) {
            //    metaData = dbRecordReader.queryMetaData();
            //}
            rs = dbRecordReader.queryData(datasourceType, splitIndex);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // 以下是没有用的，因为设置result size的时候已经fetch完了，应该设置preStatment的FetchSize
            // rs.setFetchSize(fetchSize);

            long dealRowMemSize = 0;
            long dealRowCnt = 0;
            long sendRowsCnt = 0;
            List<List<Object>> tuples = new ArrayList<>();

            long monitorTimeInterval = Constants.ZkTopoConfForFullPull.HEARTBEAT_MONITOR_TIME_INTERVAL_DEFAULT_VAL;
            String monitorTimeIntervalConf = commonProps.getProperty(Constants.ZkTopoConfForFullPull.HEARTBEAT_MONITOR_TIME_INTERVAL);
            if (monitorTimeIntervalConf != null) {
                monitorTimeInterval = Long.valueOf(monitorTimeIntervalConf);
            }

            long lastUpdatedMonitorTime = System.currentTimeMillis();

            //oracle不加这一行, 可能会产生结果集已耗尽的问题
            //rs.beforeFirst();


            String outputVersion = FullPullHelper.getOutputVersion();
            while (rs.next()) {
                List<Object> rowDataValues = new ArrayList<>();
                String pos = payloadObject.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_POS);
                rowDataValues.add(pos);
                rowDataValues.add(opTs);
                rowDataValues.add("i");
                //1.3 or later include _ums_uid_
                if (!outputVersion.equals(Constants.VERSION_12)) {
                    long uniqId = localZkService.nextValue(dbConf.buildNameSpaceForZkUidFetch(dataSourceInfo));
                    rowDataValues.add(String.valueOf(uniqId)); // 全局唯一 _ums_uid_
                }
                dealRowCnt++;
                sendRowsCnt++;
                for (int i = 1; i <= columnCount; i++) {
                    String columnTypeName = rsmd.getColumnTypeName(i);
                    // 关于时间的值需要特别处理一下。否则，可能会导致DbusMessageBuilder private void validateAndConvert(Object[] tuple)方法抛异常
                    // 例如 Year类型，库里值为2016，不做特别处理的话， 从rs读出来的值会被自动转成2016-01-01。按dbus映射规则，DbusMessageBuilder 将year按int处理时，会出错
                    // TODO: timezone
                    switch (columnTypeName) {
                        case "DATE":
                            if (rs.getObject(i) != null) {
                                if (datasourceType.equalsIgnoreCase(DbusDatasourceType.MYSQL.name()) ||
                                        datasourceType.equalsIgnoreCase(DbusDatasourceType.ORACLE.name())) {
                                    rowDataValues.add(rs.getDate(i) + " " + rs.getTime(i));
                                }
                            } else {
                                rowDataValues.add(rs.getObject(i));
                            }
                            break;
                        case "YEAR":
                            if (rs.getObject(i) != null) {
                                Date date = (Date) (rs.getObject(i));
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(date);
                                rowDataValues.add(cal.get(Calendar.YEAR));
                            } else {
                                rowDataValues.add(rs.getObject(i));
                            }
                            break;
                        case "TIME":
                            if (rs.getTime(i) != null) {
                                rowDataValues.add(rs.getTime(i).toString());
                            } else {
                                rowDataValues.add(rs.getObject(i));
                            }
                            break;
                        case "DATETIME":
                        case "TIMESTAMP":
                            if (rs.getTimestamp(i) != null) {
//                                  rowDataValues.add(rs.getTimestamp(i).toString());
                                String timeStamp = "";
                                if (datasourceType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())
                                        ) {
                                    timeStamp = toMysqlTimestampString(rs.getTimestamp(i), rsmd.getPrecision(i));
                                } else if (datasourceType.toUpperCase().equals(DbusDatasourceType.ORACLE.name())) {
                                    timeStamp = toOracleTimestampString(rs.getTimestamp(i), rsmd.getScale(i));
                                } else {
                                    throw new RuntimeException("Wrong Database type.");
                                }
                                rowDataValues.add(timeStamp);
                            } else {
                                Object val = rs.getObject(i);
                                if (datasourceType.toUpperCase().equals(DbusDatasourceType.MYSQL.name()) && rsmd.isNullable(i) != 1 && val == null) {
                                    // JAVA连接MySQL数据库，在操作值为0的timestamp类型时不能正确的处理，而是默认抛出一个异常，就是所见的：java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column 7 to TIMESTAMP。
                                    // DBUS处理策略：在JDBC连接串配置属性：zeroDateTimeBehavior=convertToNull，来避免异常。
                                    // 但当对应列约束为非空时，转换成null，后续逻辑校验通不过。所以对于mysql非空timestamp列，当得到值为null时，一定是发生了从 '0000-00-00 00:00:00'到null的转换。为了符合后续逻辑校验，此处强制将null置为'0000-00-00 00:00:00'。
                                    val = "0000-00-00 00:00:00";
                                }
                                rowDataValues.add(val);
                            }
                            break;
                        case "BINARY":
                        case "VARBINARY":
                        case "TINYBLOB":
                        case "BLOB":
                            if (rs.getObject(i) != null) {
                                // 对于上述四种类型，根据canal文档https://github.com/alibaba/canal/issues/18描述，针对blob、binary类型的数据，使用"ISO-8859-1"编码转换为string
                                // 为了和增量保持一致，对于这四种类型，全量也需做特殊处理：读取bytes并用ISO-8859-1编码转换成string。
                                // 后续DbusMessageBuilder  void validateAndConvert(Object[] tuple)方法会统一按ISO-8859-1编码处理全量/增量数据。
                                // 另，测试发现，这样的转换已“最大程度”和增量保持了一致。但对于BINARY类型，仍有一点差异。具体如下：
                                // 设数据库有一列名为filed_binay，类型为binary(200)，插入数据为： "test_binary中文测试转换,，dbus将此类型转换为base64编码 "。
                                // 在不加密的情况下，同样的数据，落到EDP mysql后，增量全量的数据能对上，如下：
                                // 增量：test_binary中文测试转换,，dbus将此类型转换为base64编码
                                // 全量：test_binary中文测试转换,，dbus将此类型转换为base64编码
                                // 在hash_md5加密的情况下，增量全量的数据对不上。数据如下：
                                // 增量：g6w
                                // 全量：??svom_u
                                // 原因：filed_binay 列类型为binary(200)，插入字符串长度没达到200，数据库将内容自动补齐至200。
                                // 增量通过canal读取原始数据时，读到的数据忽略了补齐部分。
                                // 全量通过JDBC读取原始数据时，读到的是包含补齐部分的数据，长度200。
                                // 对于这种补齐的情况，不加密处理的话，肉眼观察，内容编码/解码没区别。
                                // 用hd5加密的话，hd5加密结果会不同。
                                // 对于这个情况，暂时忽略搁置。
                                rowDataValues.add(new String(rs.getBytes(i), "ISO-8859-1"));
                            } else {
                                rowDataValues.add(rs.getObject(i));
                            }
                            break;
                        //暂时只支持BIT(0)~BIT(8)，对于其它的(n>8) BIT(n)，需要增加具体的处理
                        case "BIT":
                            byte[] value = rs.getBytes(i);
                            if (value != null && value.length > 0)
                                rowDataValues.add(value[0] & 0xFF);
                            else
                                rowDataValues.add(rs.getObject(i));
                            break;
                        default:
                            rowDataValues.add(rs.getObject(i));
                            break;
                    }
                    dealRowMemSize += String.valueOf(rs.getObject(i)).getBytes().length;
                }

                tuples.add(rowDataValues);
                if (isKafkaSend(dealRowMemSize, sendRowsCnt)) {
                    dealRowMemSize = 0;
                    sendRowsCnt = 0;
                    DbusMessage dbusMessage = buildResultMessage(outputVersion, tuples, dataSourceInfo, dbConf, rsmd,
                            metaInDbus, inputSplit.getTargetTableName(), tablePartition, batchNo);
                    //将数据写入kafka
                    sendMessageToKafka(resultKey, dbusMessage, sendCnt, recvCnt, isError);
                    tuples.clear();
                    //tuples = new ArrayList<>();
                }

                if (isError.get()) {
                    throw new Exception("kafka send exception!");
                }

                long updatedMonitorInterval = (System.currentTimeMillis() - lastUpdatedMonitorTime) / 1000;
                if (updatedMonitorInterval > monitorTimeInterval) {
                    //1. 隔一段时间刷新monitor zk状态
                    emitMonitorState(input, dataSourceInfo, progressInfoNodePath, startSecs, totalRows, totalPartitions, dealRowCnt, 0); //最后一个参数：一片尚未完成，计数0
                    long endTime = (System.currentTimeMillis() - startTime) / 1000;
                    LOG.info("dsKey: {}, pull_index{}: finished {} rows. consume time {}s", dsKey, splitIndex, dealRowCnt, endTime);
                    lastUpdatedMonitorTime = System.currentTimeMillis();
                    dealRowCnt = 0;

                    //2. 如果已经出现错误，跳过后来的tuple数据
                    progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
                    if (progressObj.getErrorMsg() != null) {
                        LOG.error("Get process failed，skipped index:" + splitIndex);
                        collector.fail(input);
                        return;
                    }
                }
            }

            /**
             * 临时的UMS_UID的zkservice需要关闭
             */
            localZkService.close();


            if (tuples.size() > 0) {
                DbusMessage dbusMessage = buildResultMessage(outputVersion, tuples, dataSourceInfo, dbConf, rsmd,
                        metaInDbus, inputSplit.getTargetTableName(), tablePartition, batchNo);
                tuples.clear();
                tuples = null;

                //发送剩余的数据到 result topic
                sendMessageToKafka(resultKey, dbusMessage, sendCnt, recvCnt, isError);
            }

            boolean isFinished = isSendFinished(sendCnt, recvCnt);
            if (isFinished) {
                emitMonitorState(input, dataSourceInfo, progressInfoNodePath, startSecs, totalRows, totalPartitions, dealRowCnt, 1);//最后一个参数：完成一片，计数1
                long endTime = (System.currentTimeMillis() - startTime) / 1000;
                LOG.info("dsKey: {}, pull_index{}: finished {} rows. consume time {}s", dsKey, splitIndex, dealRowCnt, endTime);

                collector.ack(input);
            } else {
                throw new Exception("Waiting kafka ack timeout!");
            }

            LOG.info("dsKey: {}, pull_index{} pull finished.  total {} rows", dsKey, splitIndex, dealRowCnt);
        } catch (Exception e) {
            String errorMsg = dsKey + ":Exception happened when fetching data of split: " + jsonObject.toJSONString() + "."
                    + e.getMessage() + "  [split index is " + splitIndex + "]";
            LOG.error(errorMsg, e);
            FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(),
                    Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
            collector.fail(input);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (dbManager != null) {
                    dbManager.close();
                }
            } catch (Exception e) {
                LOG.error("close dbManager error.", e);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("progressInfo"));
    }

    @SuppressWarnings("unchecked")
    private void sendMessageToKafka(String key, DbusMessage dbusMessage, AtomicLong sendCnt, AtomicLong recvCnt, AtomicBoolean isError) throws Exception {
        if (stringProducer == null) {
            throw new Exception("producer is null, can't send to kafka!");
        }

        ProducerRecord record = new ProducerRecord<>(resultTopic, key, dbusMessage.toString());
        sendCnt.getAndIncrement();
        stringProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    LOG.error("dbusMessage send to kafka exception!", e);
                    isError.set(true);
                } else {
                    recvCnt.getAndIncrement();
                }
            }
        });
    }

    private boolean isSendFinished(AtomicLong send, AtomicLong recv) {
        long startTime = System.currentTimeMillis();
        while ((send.get() > recv.get())) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            //等待1分钟仍然没有收到recv就认为写入失败
            if (System.currentTimeMillis() - startTime > 60000) {
                LOG.error("System.currentTimeMillis()-startTime  is {}", (System.currentTimeMillis() - startTime));
                return false;
            }
        }
        return true;
    }

    private DbusMessage buildResultMessage(String outputVersion, List<List<Object>> tuples, String dataSourceInfo,
                                           DBConfiguration dbConf, ResultSetMetaData rsmd, MetaWrapper metaInDbus,
                                           String seriesTableName, String tablePartition, int batchNo) throws SQLException {


        DbusMessageBuilder builder = new DbusMessageBuilder(outputVersion);
        builder.build(DbusMessage.ProtocolType.DATA_INITIAL_DATA, dbConf.getDbTypeAndNameSpace(dataSourceInfo, seriesTableName, tablePartition), batchNo);
        String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            MetaWrapper.MetaCell metaCell = metaInDbus.get(columnName);
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

        // 脱敏
        //UmsEncoder encoder = new MessageEncoder();
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
        return message;
    }

    private void emitMonitorState(Tuple input, String dataSourceInfo, String progressInfoNodePath,
                                  String startSecs, String totalRows, String totalPartitions,
                                  long finishedRow, int finishedShardCount) {
        JSONObject jsonInfo = new JSONObject();
        jsonInfo.put(DataPullConstants.DATA_SOURCE_INFO, dataSourceInfo);
        jsonInfo.put(DataPullConstants.DATA_MONITOR_ZK_PATH, progressInfoNodePath);
        jsonInfo.put(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_FINISHED_COUNT, finishedShardCount);
        jsonInfo.put(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_FINISHED_ROWS, finishedRow);
        jsonInfo.put(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_START_SECS, startSecs);
        jsonInfo.put(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_TOTAL_ROWS, totalRows);
        jsonInfo.put(DataPullConstants.DATA_CHUNK_COUNT, totalPartitions);
        collector.emit(input, new Values(jsonInfo));
    }

    private boolean isKafkaSend(long totalMemSize, long totalRows) {
        if (totalMemSize >= kafkaSendBatchSize.get() || totalRows >= kafkaSendRows.get()) {
            return true;
        }
        return false;
    }

    /* The default java.sql.Timestamp.toString() does not equal to mysql timestamp.
     * Below code lines are copied from java.sql.Timestamp.toString(), just changed the nanos part.
     */
    public String toMysqlTimestampString(java.sql.Timestamp ts, int precision) {
        // Mysql's metaData.getScale() always return 0. We use getPrecision() to estimate scale.
        // By testing, the precision of datetime is 19, which means the length "2010-01-02 10:12:23" is 19.
        // the precision of datetime(1) is 21, which means the length "2010-01-02 10:12:23.1" is 21.
        // the precision of datetime(2) is 22, which means the length "2010-01-02 10:12:23.12" is 22. and so on.
        // So, we use 20 as key to distinguish datetime from datetime(x).
        int meta = precision - 20;
        if (meta > 6) {
            throw new RuntimeException("unknow useconds meta : " + meta);
        }

        int year = ts.getYear() + 1900;
        int month = ts.getMonth() + 1;
        int day = ts.getDate();
        int hour = ts.getHours();
        int minute = ts.getMinutes();
        int second = ts.getSeconds();
        int nanos = ts.getNanos();
        String yearString;
        String monthString;
        String dayString;
        String hourString;
        String minuteString;
        String secondString;
        String nanosString;
        String zeros = "000000000";
        String yearZeros = "0000";
        StringBuffer timestampBuf;

        if (year < 1000) {
            // Add leading zeros
            yearString = "" + year;
            yearString = yearZeros.substring(0, (4 - yearString.length())) +
                    yearString;
        } else {
            yearString = "" + year;
        }
        if (month < 10) {
            monthString = "0" + month;
        } else {
            monthString = Integer.toString(month);
        }
        if (day < 10) {
            dayString = "0" + day;
        } else {
            dayString = Integer.toString(day);
        }
        if (hour < 10) {
            hourString = "0" + hour;
        } else {
            hourString = Integer.toString(hour);
        }
        if (minute < 10) {
            minuteString = "0" + minute;
        } else {
            minuteString = Integer.toString(minute);
        }
        if (second < 10) {
            secondString = "0" + second;
        } else {
            secondString = Integer.toString(second);
        }

        //make nanoString length as 9.
        if (nanos == 0) {
            nanosString = zeros.substring(0, 9);
        } else {
            nanosString = Integer.toString(nanos);
        }

        // Add leading zeros
        nanosString = zeros.substring(0, (9 - nanosString.length())) +
                nanosString;

        if (meta <= 0) {
            nanosString = "";
        } else {
            //truncate nanoString by meta
            nanosString = "." + nanosString.substring(0, meta);
        }

        // do a string buffer here instead.
        timestampBuf = new StringBuffer(20 + nanosString.length());
        timestampBuf.append(yearString);
        timestampBuf.append("-");
        timestampBuf.append(monthString);
        timestampBuf.append("-");
        timestampBuf.append(dayString);
        timestampBuf.append(" ");
        timestampBuf.append(hourString);
        timestampBuf.append(":");
        timestampBuf.append(minuteString);
        timestampBuf.append(":");
        timestampBuf.append(secondString);
        timestampBuf.append(nanosString);

        return (timestampBuf.toString());
    }


    /* The default java.sql.Timestamp.toString() does not equal to oracle timestamp.
     * Eg: 28-NOV-16 11.33.33.123000 AM will be transformed to 2016-11-28 11:33:33.123,  instead of 2016-11-28 11:33:33.123000
     * Below codes let the transformed result is equaled original data exactly.
     */
    public String toOracleTimestampString(java.sql.Timestamp ts, int scale) {
        String timeStamp = ts.toString();
        String timeStampLastPart = timeStamp.substring(timeStamp.lastIndexOf(".") + 1, timeStamp.length());
        int needAdd = scale - timeStampLastPart.length();
        while (needAdd > 0) {
            timeStamp += "0";
            needAdd--;
        }
        return timeStamp;
    }

    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        String loadResultMsg = null;
        try {
            this.confMap = FullPullHelper.loadConfProps(zkconnect, topologyId, zkTopoRoot, null);
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.dsName = commonProps.getProperty(Constants.ZkTopoConfForFullPull.DATASOURCE_NAME);
            this.stringProducer = (Producer) confMap.get(FullPullHelper.RUNNING_CONF_KEY_STRING_PRODUCER);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            this.stringProducerProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_STRING_PRODUCER_PROPS);

            String sendBatchSizeStr = stringProducerProps.getProperty(DataPullConstants.KAFKA_SEND_BATCH_SIZE);
            String sendRowsStr = stringProducerProps.getProperty(DataPullConstants.KAFKA_SEND_ROWS);
            if (StringUtils.isNotBlank(sendBatchSizeStr) && (Long.valueOf(sendBatchSizeStr) != kafkaSendBatchSize.get())) {
                kafkaSendBatchSize.set(Long.valueOf(sendBatchSizeStr));
            }
            if (StringUtils.isNotBlank(sendRowsStr) && (Long.valueOf(sendRowsStr) != kafkaSendRows.get())) {
                kafkaSendRows.set(Long.valueOf(sendRowsStr));
            }
            loadResultMsg = "Running Config is " + notifyEvtName + " successfully for PagedBatchDataFetchingBolt!";
            LOG.info(loadResultMsg);

            //初始化脱敏插件配置信息
            PluginManagerProvider.initialize(new FullPullPluginLoader());
            LOG.info("FullPullEncodePlugin init success ");
        } catch (Exception e) {
            loadResultMsg = e.getMessage();
            LOG.error(notifyEvtName + "ing running configuration encountered Exception!", loadResultMsg);
            throw e;
        } finally {
            if (reloadMsgJson != null) {
                FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-dataFetching-bolt", false, zkconnect);
                //reload脱敏类型
                PluginManagerProvider.reloadManager();
                LOG.info("FullPullEncodePlugin reload success ");
            }
        }
    }

    private ZkService reloadZkServiceRunningConf() {
        try {
            Map confMap = FullPullHelper.reloadZkServiceConfProps(zkconnect, zkTopoRoot);
            LOG.info("ZkService reload success for ums_uid");
            return (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
        } catch (Exception e) {
            LOG.error("ZkService reload failed, error message : {}", e);
        }
        return null;
    }

}
