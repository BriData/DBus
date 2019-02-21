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

package com.creditease.dbus.log.processor.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.commons.log.processor.adapter.LogFilebeatAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogFlumeAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogStashAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogUmsAdapter;
import com.creditease.dbus.commons.log.processor.parse.Field;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.log.processor.base.LogProcessorBase;
import com.creditease.dbus.log.processor.dao.IProcessorLogDao;
import com.creditease.dbus.log.processor.dao.impl.ProcessorLogDao;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.util.DateUtil;
import com.creditease.dbus.log.processor.vo.RecordWrapper;
import com.creditease.dbus.log.processor.vo.RuleInfo;
import com.creditease.dbus.log.processor.window.HeartBeatWindowInfo;
import com.creditease.dbus.log.processor.window.StatWindowInfo;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class LogProcessorTransformBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorTransformBolt.class);

    private OutputCollector collector = null;
    private LogProcessorTransformBoltInner inner = null;
    private TopologyContext context = null;
    Map<String, Long> statMap = new HashMap<>();
    Map<String, Long> tableErrorStatMap = new HashMap<>();
    Map<String, Long> globalMissStat = new HashMap<>();
    Long readKafkaRecordCount = 0L;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        inner = new LogProcessorTransformBoltInner(conf);
        init();
        logger.info("LogProcessorTransformBolt is started!");
    }

    private void init() {
        inner.loadConf();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String emitDataType = (String) input.getValueByField("emitDataType");
            switch (emitDataType) {
                case Constants.EMIT_DATA_TYPE_CTRL: {
                    ConsumerRecord<String, byte[]> record = (ConsumerRecord<String, byte[]>) input.getValueByField("records");
                    processControlCommand(record, input);
                    break;
                }
                case Constants.EMIT_DATA_TYPE_HEARTBEAT: {
                    Map<String, String> recordMap = null;
                    ConsumerRecord<String, byte[]> record;
                    Integer partition = null;
                    Long offset = null;
                    if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)
                            || DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FILEBEAT)
                            || DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FLUME)) {
                        recordMap = (HashMap) input.getValueByField("records");
                        partition = Integer.valueOf(recordMap.get("partition"));
                        offset = Long.valueOf(recordMap.get("offset"));
                    } else if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH ) ||
                            DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                        record = (ConsumerRecord<String, byte[]>) input.getValueByField("records");
                        recordMap = JSON.parseObject(new String(record.value(), "UTF-8"), HashMap.class);
                        partition = record.partition();
                        offset = record.offset();
                    }
                    emitHbOrStatInfo(input, recordMap, partition, offset);
                    break;
                }
                case Constants.EMIT_DATA_TYPE_NORMAL: {
                    List<ConsumerRecord<String, byte[]>> normalRecords = (List<ConsumerRecord<String, byte[]>>) input.getValueByField("records");
                    Map<String, List<RecordWrapper>> tableDatasMap = new HashMap<>();
                    long startTime = System.currentTimeMillis();
                    long count_wk = 0l;
                    for (ConsumerRecord<String, byte[]> record : normalRecords) {
                        Iterator<String> it = null;
                        if(DbusDatasourceType.stringEqual(inner.dbusDsConf.getDsType(), DbusDatasourceType.LOG_UMS)) {
                            it = new LogUmsAdapter(new String(record.value(), "UTF-8"));
                        } else if(DbusDatasourceType.stringEqual(inner.dbusDsConf.getDsType(), DbusDatasourceType.LOG_LOGSTASH)
                                || DbusDatasourceType.stringEqual(inner.dbusDsConf.getDsType(), DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                            it = new LogStashAdapter(new String(record.value(), "UTF-8"));
                        } else if(DbusDatasourceType.stringEqual(inner.dbusDsConf.getDsType(), DbusDatasourceType.LOG_FLUME)) {
                            it = new LogFlumeAdapter(record.key(), new String(record.value(), "UTF-8"));
                        } else if(DbusDatasourceType.stringEqual(inner.dbusDsConf.getDsType(), DbusDatasourceType.LOG_FILEBEAT)) {
                            it = new LogFilebeatAdapter(new String(record.value(), "UTF-8"));
                        }
                        while (it.hasNext()) {
                            boolean missFlag = true;
                            String value = it.next();
                            String host = null;
                            Map<String, String> recordMap = JSON.parseObject(value, HashMap.class);
                            //logstash、log_flume或者logstash_json的host信息
                            if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH) ||
                                    DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH_JSON) ||
                                            DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FLUME)) {
                                host = StringUtils.replaceChars(recordMap.get("host"), ".", "_");
                            }
                            //ums中namespace拼接成的host信息
                            if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
                                String namespace = recordMap.get("namespace");
                                String[] vals = StringUtils.split(namespace, ".");
                                host = StringUtils.joinWith("_", vals[0], vals[1], vals[2], vals[4]);
                                recordMap.put("umsSource", vals[4]);
                            }
                            //filebeat中的host信息
                            if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FILEBEAT)) {
                                host = StringUtils.replaceChars(recordMap.get("beat.hostname"), ".", "_");
                            }
                            boolean tableExecuteFlag = false;
                            //表
                            for (Map.Entry<Long, Map<Long, List<RuleInfo>>> tableGroupRules : inner.rules.entrySet()) {
                                String dsName = StringUtils.EMPTY;
                                String schemaName = StringUtils.EMPTY;
                                String tableName = StringUtils.EMPTY;
                                Long version = 0L;
                                List<RecordWrapper> tableDatas = new ArrayList<>();
                                //组
                                for (Map.Entry<Long, List<RuleInfo>> groupRules : tableGroupRules.getValue().entrySet()) {
                                    List<String> groupValues = new ArrayList<>();
                                    groupValues.add(value);
                                    int idx = 0;
                                    String namespace = null;
                                    //规则
                                    for (RuleInfo ri : groupRules.getValue()) {
                                        if (idx++ == 0) {
                                            dsName = ri.getDsName();
                                            schemaName = ri.getSchemaName();
                                            tableName = ri.getTableName();
                                            version = (ri.getVersion() == null) ? version : ri.getVersion();
                                            namespace = StringUtils.joinWith("|", host, dsName, schemaName, tableName, version);
                                        }
                                        try {
                                            Rules rule = Rules.fromStr(ri.getRuleTypeName());
                                            List<RuleGrammar> ruleGrammarList = JSON.parseArray(ri.getRuleGrammar(), RuleGrammar.class);
                                            groupValues = rule.getRule().transform(groupValues, ruleGrammarList, rule);
                                            if (groupValues.isEmpty())
                                                break;
                                        } catch (Exception e) {
                                            logger.error("parse rule failed on LogProcessorTransformBolt! table: {}, Exception: {}", namespace, e);
                                            // 统计错误计数
                                            if (tableErrorStatMap.containsKey(namespace)) {
                                                tableErrorStatMap.put(namespace, tableErrorStatMap.get(namespace) + 1);
                                            } else {
                                                tableErrorStatMap.put(namespace, 1L);
                                            }
                                            //出现异常时，groupValues不一定为空
                                            groupValues.clear();
                                        }
                                    }

                                    if (!groupValues.isEmpty()) {
                                        RecordWrapper rw = new RecordWrapper();
                                        if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
                                            rw.setTimeStamp(DateUtil.convertStrToLong4Date(recordMap.get("ums_ts_"), inner.logProcessorConf.getProperty("ums.timestamp")));
                                        }
                                        rw.setRecordMap(recordMap);
                                        rw.setOffset(record.offset());
                                        rw.setPartition(record.partition());
                                        rw.setValue(groupValues);
                                        tableDatas.add(rw);
                                        //单表，当正常处理时，如果一条record满足某个group时，直接跳过后面的group
                                        if(StringUtils.equals("false", inner.logProcessorConf.getProperty("multiTable"))) {
                                            tableExecuteFlag = true;
                                            break;
                                        }
                                    }
                                }
                                if (!tableDatas.isEmpty()) {
                                    //当record满足某张表时，表示该record不属于miss的
                                    missFlag = false;
                                    String key = StringUtils.joinWith("|", host, dsName, schemaName, tableName, version);
                                    if (tableDatasMap.containsKey(key)) {
                                        tableDatasMap.get(key).addAll(tableDatas);
                                    } else {
                                        tableDatasMap.put(key, tableDatas);
                                    }
                                    // 对于UMS数据，统计扁平化之后的数据条数
                                    if (statMap.containsKey(key)) {
                                        statMap.put(key, statMap.get(key) + tableDatas.size());
                                    } else {
                                        statMap.put(key, Long.valueOf(String.valueOf(tableDatas.size())));
                                    }
                                }
                                if(tableExecuteFlag) break;
                            }
                            //ums中的heartbeat数据和ums数据的格式是一样的，统计中要去掉心跳数据
                            if(missFlag && !StringUtils.contains(record.key(), "data_increment_heartbeat")) {
                                // 对于UMS数据，统计扁平化之后的数据条数，和表无关
                                if (globalMissStat.containsKey(host)) {
                                    globalMissStat.put(host, globalMissStat.get(host) + 1);
                                } else {
                                    globalMissStat.put(host, 1L);
                                }
                            }
                        }
                        readKafkaRecordCount ++;
                        count_wk ++;
                    }
                    emitUmsData(input, tableDatasMap);
                    if (logger.isDebugEnabled())
                        logger.debug("transform process count:{} cost time: {}", count_wk, System.currentTimeMillis() - startTime);
                    break;
                }
                default :
                    break;
            }
            collector.ack(input);
        } catch (Exception e) {
            logger.error("LogProcessorTransformBolt execute error:", e);
            collector.fail(input);
            collector.reportError(e);
        }
    }

    private void emitHbOrStatInfo(Tuple input, Map<String, String> recordMap, Integer partition, Long offset) {
        Long timestamp;
        //心跳数据
        if (StringUtils.equals(recordMap.get("type"),
                inner.logProcessorConf.getProperty(Constants.LOG_HEARTBEAT_TYPE))) {
            //时间戳：logstash或ums的时间戳
            timestamp = getLogTimestamp(recordMap);
            String host = StringUtils.replaceChars(recordMap.get("host"), ".", "_");
            String tableName = null;
            String umsSource = null;
            if(!StringUtils.isEmpty(recordMap.get("tableName"))) {
                tableName = recordMap.get("tableName");
                umsSource = recordMap.get("umsSource");
            }
            //发送active表的统计信息
            String namespace = emitActiveTableInfo(input, timestamp, host, partition, offset, tableName, umsSource);
            //发送abort表的统计信息
            if(StringUtils.isEmpty(namespace)) {
                namespace = emitAbortTableInfo(input, timestamp, host, partition, offset, tableName, umsSource);
            } else {
                emitAbortTableInfo(input, timestamp, host, partition, offset, tableName, umsSource);
            }
            //发送全局统计信息
            emitGlobalStatInfo(input, timestamp, host, namespace);
        }
    }

    private void emitUmsData(Tuple input, Map<String, List<RecordWrapper>> tableDatasMap) throws Exception {
        for(Map.Entry<String, List<RecordWrapper>> entry : tableDatasMap.entrySet()) {
            // vals[0]:host, vals[1]: dsName, vals[2]: schemaName, vals[3]: tableName, vals[4]: version
            String[] vals = StringUtils.split(entry.getKey(), "|");
            String host = vals[0];
            if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
                host = entry.getValue().get(0).getRecordMap().get("umsSource");
            }
            DbusMessage ums = buildUms(entry.getValue(), vals[1], vals[2], vals[3], Long.parseLong(vals[4]), host);
            String tableKey = StringUtils.joinWith("|", vals[1], vals[2], vals[3], vals[4]);
            // "outputTopic", "table", "value", "emitDataType"
            collector.emit("umsStream", input,
                    new Values(inner.activeTableToTopicMap.get(tableKey),
                            tableKey, ums, Constants.EMIT_DATA_TYPE_NORMAL));
        }
    }

    private Long getLogTimestamp(Map<String, String> recordMap) {
        Long timestamp = null;
        if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
            timestamp = Long.valueOf(recordMap.get("@timestamp"));
        } if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH)
                || DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH_JSON)) {
            timestamp = DateUtil.addDay(recordMap.get("@timestamp"), inner.logProcessorConf.getProperty("@timestamp"), Integer.valueOf(inner.logProcessorConf.getProperty("add.hours")));
        } else if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FLUME)) {
            timestamp = DateUtil.addDay(recordMap.get("@timestamp"), inner.logProcessorConf.getProperty("flume.timestamp"), 0);
        } else if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FILEBEAT)) {
            timestamp = DateUtil.addDay(recordMap.get("@timestamp"), inner.logProcessorConf.getProperty("dbus.heartbeat.timestamp"), 0);
        }
        return timestamp;
    }

    private void processControlCommand(ConsumerRecord<String, byte[]> record, Tuple input) {
        try {
            String json = new String(record.value(), "utf-8");
            ControlType cmd = ControlType.getCommand(JSONObject.parseObject(json).getString("type"));
            switch (cmd) {
                case LOG_PROCESSOR_RELOAD_CONFIG: {
                    logger.info("LogProcessorTransformBolt-{} 收到reload消息！Type: {}, Values: {} " , context.getThisTaskId(), cmd, json);
                    inner.close(true);
                    init();
                    inner.zkHelper.saveReloadStatus(json, "LogProcessorTransformBolt-" + context.getThisTaskId(), true);
                    break;
                }
                case APPENDER_TOPIC_RESUME: {
                    JSONObject jsonObject = (JSONObject)JSON.parseObject(json).get("payload");
                    logger.info("LogProcessorTransformBolt-{} 收到resume消息！Type: {}, Values: {} " , context.getThisTaskId(), cmd, JSON.toJSONString(jsonObject));
                    String[] vals = StringUtils.split(jsonObject.getString("topic"), ".");
                    String dsName = vals[0];
                    String schemaName = jsonObject.getString("SCHEMA_NAME");
                    String tableName = jsonObject.getString("TABLE_NAME");
                    String status = jsonObject.getString("STATUS");
                    IProcessorLogDao dao = new ProcessorLogDao();
                    dao.updateTableStatus(Constants.DBUS_CONFIG_DB_KEY, dsName, schemaName, tableName, status);
                    inner.loadConf();
                    break;
                }
                default:
                    break;
            }

            if (context.getThisTaskIndex() == 0) {
                collector.emit("ctrlStream", input, new Values(json, Constants.EMIT_DATA_TYPE_CTRL));
            }
        } catch (Exception e) {
            logger.error("LogProcessorTransformBolt processControlCommand():", e);
            collector.reportError(e);
            collector.fail(input);
        }
    }


    private DbusMessage buildUms(List<RecordWrapper> tableDatas, String dsName, String schema, String table, long version, String host) throws Exception {
        DbusMessageBuilder builder = new DbusMessageBuilder();
        String namespace = buildNameSpace(dsName, schema, table, Long.valueOf(version).intValue(), host);
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, 0);

        int idx = 0;
        for (RecordWrapper data : tableDatas) {
            // 设置schema和payload
            /**
             * 1.UMS_TS : meta中的时间戳，即@timestamp。
             *
             * 2.UMS_ID:
             * 64位 = 1位符号 + 41位毫秒数 + 4位partition + 18位offset mod
             * 毫秒数为 当前毫秒 – 1483200000000（2017-1-1的毫秒数据），就是意味系统支持时间最大为2086/9/7 15:47:35
             * Partition支持16个partition Offset 最多262143（每毫秒每个partiion最多支持262143条数据）
             *
             3.UMS_UID：从zookeeper中获取，参考增量的设计。
             */
            Long timeStamp = null;
            //ums时间戳不需要转换时区
            if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_UMS)) {
                timeStamp = data.getTimeStamp();
            }
            //logstash和filebeat需要转换时区
            else if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH)
                    || DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_LOGSTASH_JSON)
                    || DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FILEBEAT)){
                //@timestamp ：抽取时间，只使用@timestamp
                if(StringUtils.isEmpty(data.getRecordMap().get("@timestamp"))) logger.error("@timestamp is null!");
                timeStamp = DateUtil.addDay(data.getRecordMap().get("@timestamp"),
                        inner.logProcessorConf.getProperty("@timestamp"), Integer.valueOf(inner.logProcessorConf.getProperty("add.hours")));
            } else if(DbusDatasourceType.stringEqual(inner.logProcessorConf.getProperty("log.type"), DbusDatasourceType.LOG_FLUME)) {
                timeStamp = Long.valueOf(data.getRecordMap().get("timestamp"));
            }

            try {
                List<Object> values = new ArrayList<>();
                Long ums_id = (timeStamp - 1483200000000L) << 22 | (data.getPartition() << 18) | (data.getOffset() % 262144);
                values.add(ums_id);
                values.add(DateUtil.convertLongToStr4Date(timeStamp));
                values.add("i");
                values.add(String.valueOf(inner.zkHelper.getZkservice().nextValue(buildNameSpaceForZkUidFetch(dsName))));

                // 设置其它field
                for (String strField : data.getValue()) {
                    Field field = JSON.parseObject(strField, Field.class);
                    if (idx == 0)
                        builder.appendSchema(field.getName(), convertProcessorLogDataType(field.getType()), false);
                    values.add(field.getValue());
                }
                builder.appendPayload(values.toArray());
            } catch (IllegalArgumentException e) {
                logger.error("IllegalArgumentException : {}", e);
                logger.info("schema info: {}", builder.getMessage().getSchema().toString());
                logger.info("values info: {}", data.getValue().toString());
            } catch (Exception e) {
                logger.error("生成ums出错：{}", e);
            }
            idx++;
        }
        return builder.getMessage();
    }

    private String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver, String host) {
        return Joiner.on(".").join(inner.logProcessorConf.getProperty("log.type"), datasourceNs, dbSchema, table, ver, host, "0");
    }

    private String buildNameSpaceForZkUidFetch(String dsName) {
        return StringUtils.join(new String[] {dsName, "schema.table.version"}, ".");
    }


    private DataType convertProcessorLogDataType(String type) {
        type = type.toUpperCase();
        DataType datatype;
        switch (type) {
            case "LONG":
                datatype = DataType.LONG;
                break;
            case "DOUBLE":
                datatype = DataType.DOUBLE;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                datatype = DataType.DATETIME;
                break;
            default:
                datatype = DataType.STRING;
                break;
        }
        return datatype;
    }

    private String emitActiveTableInfo(Tuple input, Long timestamp, String host, Integer partition, Long offset, String tableName, String umsSource) {
        String namespace = null;
        for(Map.Entry<String, String> entry : inner.activeTableToTopicMap.entrySet()) {
            String[] vals = StringUtils.split(entry.getKey(), "|");
            namespace = entry.getKey();
            if(StringUtils.isEmpty(tableName)) {
                namespace = emitActiveTableHbAndStatInfo(input, timestamp, host, partition, offset, umsSource, entry);
            } else if(StringUtils.equals(tableName, vals[2])) {
                namespace = emitActiveTableHbAndStatInfo(input, timestamp, host, partition, offset, umsSource, entry);
            } else {
                continue;
            }
        }
        return namespace;
    }

    private String emitActiveTableHbAndStatInfo(Tuple input, Long timestamp, String host, Integer partition, Long offset, String umsSource, Map.Entry<String, String> entry) {
        String namespace;
        //entry Key: ds, schema, table, version
        namespace = entry.getKey();
        StatWindowInfo st = new StatWindowInfo();
        st.setTable(entry.getKey());
        st.setTaskId(context.getThisTaskId());
        st.setTaskIndex(context.getThisTaskIndex());
        st.setTimestamp(timestamp);
        st.setNamespace(entry.getKey());
        st.setHost(host);
        st.setReadKafkaCount(readKafkaRecordCount);
        readKafkaRecordCount = 0L;

        HeartBeatWindowInfo hb = new HeartBeatWindowInfo();
        hb.setTable(entry.getKey());
        hb.setTaskId(context.getThisTaskId());
        hb.setTaskIndex(context.getThisTaskIndex());
        hb.setTimestamp(timestamp);
        hb.setNamespace(entry.getKey());
        hb.setPartition(partition);
        hb.setOffset(offset);
        hb.setStatus("ok");
        hb.setOutputTopic(inner.activeTableToTopicMap.get(entry.getKey()));
        hb.setHost(host);
        hb.setUmsSource(umsSource);

        String statKey = StringUtils.joinWith("|", host, entry.getKey());
        if(statMap.containsKey(statKey)) {
            st.setSuccessCnt(statMap.get(statKey));
            statMap.remove(statKey);
        } else {
            st.setSuccessCnt(0L);
        }
        if(tableErrorStatMap.containsKey(statKey)) {
            st.setErrorCnt(tableErrorStatMap.get(statKey));
            tableErrorStatMap.remove(statKey);
        } else {
            st.setErrorCnt(0L);
        }
        collector.emit("statStream", input, new Values(st, Constants.EMIT_DATA_TYPE_NORMAL));
        collector.emit("heartbeatStream", input, new Values(hb, Constants.EMIT_DATA_TYPE_HEARTBEAT));
        return namespace;
    }

    private String emitAbortTableInfo(Tuple input, Long timestamp, String host, Integer partition, Long offset, String tableName, String umsSource) {
        String namespace = null;
        for(Map.Entry<String, String> entry : inner.abortTableToTopicMap.entrySet()) {
            String[] vals = StringUtils.split(entry.getKey(), "|");
            namespace = entry.getKey();
            if(StringUtils.isEmpty(tableName)) {
                namespace = emitAbortTableHbAndStatInfo(input, timestamp, host, partition, offset, entry);
            } else if(StringUtils.equals(tableName, vals[2])) {
                namespace = emitAbortTableHbAndStatInfo(input, timestamp, host, partition, offset, entry);
            } else {
                continue;
            }

        }
        return namespace;
    }

    private String emitAbortTableHbAndStatInfo(Tuple input, Long timestamp, String host, Integer partition, Long offset, Map.Entry<String, String> entry) {
        String namespace;//entry Key: ds, schema, table, version
        namespace = entry.getKey();
        StatWindowInfo st = new StatWindowInfo();
        st.setTable(entry.getKey());
        st.setTaskId(context.getThisTaskId());
        st.setTaskIndex(context.getThisTaskIndex());
        st.setTimestamp(timestamp);
        st.setNamespace(entry.getKey());
        st.setHost(host);
        st.setSuccessCnt(0L);

        HeartBeatWindowInfo hb = new HeartBeatWindowInfo();
        hb.setTable(entry.getKey());
        hb.setTaskId(context.getThisTaskId());
        hb.setTaskIndex(context.getThisTaskIndex());
        hb.setTimestamp(timestamp);
        hb.setNamespace(entry.getKey());
        hb.setPartition(partition);
        hb.setOffset(offset);
        hb.setStatus("abort");
        hb.setOutputTopic(inner.abortTableToTopicMap.get(entry.getKey()));
        hb.setHost(host);

        collector.emit("statStream", input, new Values(st, Constants.EMIT_DATA_TYPE_NORMAL));
        collector.emit("heartbeatStream", input, new Values(hb, Constants.EMIT_DATA_TYPE_HEARTBEAT));
        return namespace;
    }

    private void emitGlobalStatInfo(Tuple input, Long timestamp, String host, String namespace) {
        String[] vals = StringUtils.split(namespace, "|");
        StatWindowInfo st = new StatWindowInfo();
        st.setTable("_unknown_table_");
        st.setTaskId(context.getThisTaskId());
        st.setTaskIndex(context.getThisTaskIndex());
        st.setTimestamp(timestamp);
        st.setNamespace(StringUtils.joinWith("|", vals[0], vals[1], "_unknown_table_"));
        st.setHost(host);

        if(globalMissStat.containsKey(host)) {
            st.setSuccessCnt(globalMissStat.get(host));
            globalMissStat.remove(host);
        } else {
            st.setSuccessCnt(0L);
        }
        collector.emit("statStream", input, new Values(st, Constants.EMIT_DATA_TYPE_NORMAL));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("umsStream", new Fields("outputTopic", "table", "value", "emitDataType"));
        declarer.declareStream("statStream", new Fields("value", "emitDataType"));
        declarer.declareStream("heartbeatStream", new Fields("value", "emitDataType"));
        declarer.declareStream("ctrlStream", new Fields("value", "emitDataType"));
    }

    @Override
    public void cleanup() {
        inner.close(false);
    }

    private class LogProcessorTransformBoltInner extends LogProcessorBase {
        public LogProcessorTransformBoltInner(Map conf) {
            super(conf);
        }
    }

}
