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


package com.creditease.dbus.log.processor.spout;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.log.processor.base.LogProcessorBase;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.vo.AckMsg;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class LogProcessorKafkaReadSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorKafkaReadSpout.class);

    private SpoutOutputCollector collector = null;
    private LogProcessorSpoutInner inner = null;
    private TopologyContext context = null;

    private int emitCount = 0;
    private int MAX_EMIT_COUNT = 0;
    private Properties kafkaConsumerConf = null;
    private KafkaConsumer<String, byte[]> consumer = null;
    private Map<String, TopicPartition> topicPartitionMap = new HashMap<>();
    private Map<String, Long> failOffsetMap = new HashMap<>();
    private Map<String, List<TopicPartition>> topicMap = new HashMap();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        inner = new LogProcessorSpoutInner(conf);
        context.addTaskHook(new BaseTaskHook() {
            @Override
            public void spoutFail(SpoutFailInfo info) {
                super.spoutFail(info);
                AckMsg ack = (AckMsg) info.messageId;
                logger.info("hook ack info:{} latencyMs: {}", ack.getMaxOffset() + ", " + ack.getMinOffset() + ", " + ack.getPartition(),
                        info.failLatencyMs);
            }

            @Override
            public void spoutAck(SpoutAckInfo info) {
                super.spoutAck(info);
            }
        });
        init();
        logger.info("LogProcessorKafkaReadSpout is started!");
    }

    @Override
    public void nextTuple() {
        try {
            // 限速
            if (speedLimit()) return;

            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            Map<Integer, List<ConsumerRecord<String, byte[]>>> partitionRecordMap = new HashMap<>();
            for (ConsumerRecord<String, byte[]> record : records) {
                String strValue = new String(record.value(), "UTF-8");
                if (logger.isDebugEnabled())
                    logger.debug("record value: {}", strValue);

                // 处理控制命令
                if (StringUtils.equals(record.topic(), inner.dbusDsConf.getControlTopic())) {
                    processControlCommand(record);
                    emitCtrlData(record);
                    continue;
                }

                //处理UMS heartbeat
                if (DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_UMS)
                        && StringUtils.contains(record.key(), "data_increment_heartbeat")) {
                    String[] vals = StringUtils.split(record.key(), ".");
                    String[] times = StringUtils.split(vals[8], "|");
                    String txTime = times[1];
                    String tableName = vals[4];
                    String umsSource = vals[6];
                    String host = StringUtils.joinWith("_", vals[2], vals[3], vals[4], vals[6]);
                    emitData(partitionRecordMap);
                    emitUmsHbData(record, txTime, host, tableName, umsSource);
                    continue;
                }

                //处理logstash heartbeat
                if (DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_LOGSTASH)
                        || DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                    Map<String, String> recordMap = JSON.parseObject(strValue, HashMap.class);
                    if (StringUtils.equals(recordMap.get("type"), inner.logProcessorConf.getProperty(Constants.LOG_HEARTBEAT_TYPE))) {
                        // 先把心跳之前的数据发送出去
                        emitData(partitionRecordMap);
                        // 发送心跳
                        emitHbData(record);
                        // 结束本次loop
                        continue;
                    }
                }

                //处理flume heartbeat
                try {
                    if (DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_FLUME) ||
                            DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_JSON)) {
                        if (StringUtils.contains(JSON.parseObject(strValue).getString("type"), inner.logProcessorConf.getProperty(Constants.LOG_HEARTBEAT_TYPE))) {
                            // 先把心跳之前的数据发送出去
                            emitData(partitionRecordMap);
                            // 发送心跳
                            emitFlumeHbData(record);
                            // 结束本次loop
                            continue;
                        }
                    }
                } catch (Exception e) {
                    logger.error("parse string: {} failed!", strValue);
                    continue;
                }

                //处理filebeat heartbeat
                if (DbusDatasourceType.stringEqual((String) inner.logProcessorConf.get("log.type"), DbusDatasourceType.LOG_FILEBEAT)) {
                    String hbType = JSON.parseObject(new String(record.value(), "UTF-8")).getString("type");
                    if (StringUtils.contains(hbType, inner.logProcessorConf.getProperty(Constants.LOG_HEARTBEAT_TYPE))) {
                        // 先把心跳之前的数据发送出去
                        emitData(partitionRecordMap);
                        // 发送心跳
                        emitFilebeatHbData(record);
                        // 结束本次loop
                        continue;
                    }
                }

                // 把获取到的所有的record按照partition分组组合然后发送
                if (!partitionRecordMap.containsKey(record.partition())) {
                    List<ConsumerRecord<String, byte[]>> list = new ArrayList<>();
                    list.add(record);
                    partitionRecordMap.put(record.partition(), list);
                } else {
                    partitionRecordMap.get(record.partition()).add(record);
                }
            }
            // 按照partiton分组发送数据
            emitData(partitionRecordMap);
        } catch (Exception e) {
            logger.error("LogProcessorSpout nextTuple error:", e);
            collector.reportError(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("ctrlOrHbStream", new Fields("records", "emitDataType"));
        declarer.declareStream("umsStream", new Fields("records", "emitDataType"));
    }

    @Override
    public void ack(Object msgId) {
        try {
            AckMsg ack = (AckMsg) msgId;
            logger.debug("record ack success! topic: {}, partition: {}, minOffset: {}, maxOffset:{}", ack.getTopic(), ack.getPartition(), ack.getMinOffset(), ack.getMaxOffset());
            String key = StringUtils.joinWith("_", ack.getTopic(), ack.getPartition());
            if (StringUtils.equals(ack.getTopic(), inner.dbusDsConf.getControlTopic())) {
                commit(ack, key);
            } else {
                emitCount--;
                long failOffset = failOffsetMap.get(key);
                if (failOffset == -1) {
                    commit(ack, key);
                } else if (ack.getMaxOffset() < failOffset) {
                    commit(ack, key);
                }
            }
            // System.out.println(String.format("ackInfo: topic:%s, partition:%s, offset:%s", ack.getTopic(), ack.getPartition(), ack.getOffset()));
        } catch (Exception e) {
            logger.error("LogProcessorSpout ack error:", e);
            collector.reportError(e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            AckMsg ack = (AckMsg) msgId;
            logger.info("record ack failed! topic: {}, partition: {}, minOffset: {}, maxOffset:{}", ack.getTopic(), ack.getPartition(), ack.getMinOffset(), ack.getMaxOffset());
            String key = StringUtils.joinWith("_", ack.getTopic(), ack.getPartition());
            if (StringUtils.equals(ack.getTopic(), inner.dbusDsConf.getControlTopic())) {
                commit(ack, key);
            } else {
                emitCount--;
                long failOffset = failOffsetMap.get(key);
                if (failOffset == -1 || failOffset >= ack.getMinOffset()) {
                    failOffsetMap.put(key, ack.getMinOffset());
                    rollback(ack, key);
                }
            }
        } catch (Exception e) {
            logger.error("LogProcessorSpout fail error:", e);
            collector.reportError(e);
        }
    }

    @Override
    public void close() {
        inner.close(false);
        if (consumer != null) consumer.close();
    }

    private boolean speedLimit() {
        if (emitCount >= MAX_EMIT_COUNT) {
            try {
                logger.info("execute spout speed limit! emitCount: {} , MAX_EMIT_COUNT: {}", emitCount, MAX_EMIT_COUNT);
                TimeUnit.MILLISECONDS.sleep(Long.parseLong(inner.logProcessorConf.getProperty(Constants.LOG_SPEED_LIMIT_INTERVAL)));
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            return true;
        }
        return false;
    }

    private void init() {
        inner.loadConf();
        initConsumer();
        seekOffset();
        this.MAX_EMIT_COUNT = Integer.parseInt(inner.logProcessorConf.getProperty(Constants.LOG_MAX_EMIT_COUNT));
    }

    private void assignTopics(List<PartitionInfo> topicInfo, List<TopicPartition> assignTopics) {
        for (PartitionInfo pif : topicInfo) {
            TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
            assignTopics.add(tp);
            String key = StringUtils.joinWith("_", pif.topic(), String.valueOf(pif.partition()));
            topicPartitionMap.put(key, tp);
            failOffsetMap.put(key, -1L);
            if (!topicMap.containsKey(pif.topic())) {
                List<TopicPartition> partitions = new ArrayList<>();
                partitions.add(tp);
                topicMap.put(pif.topic(), partitions);
            } else {
                topicMap.get(pif.topic()).add(tp);
            }
        }
    }

    private void commit(AckMsg ack, String key) {
        TopicPartition tp = topicPartitionMap.get(key);
        if (tp != null) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp, new OffsetAndMetadata(ack.getMaxOffset()));
            consumer.commitSync(offsets);
        } else {
            logger.error(String.format("do not find key:%s of TopicPartition", key));
        }
    }

    private void rollback(AckMsg ack, String key) {
        TopicPartition tp = topicPartitionMap.get(key);
        if (tp != null) {
            logger.info(String.format("seek topic: %s to offset: %d", tp.topic(), ack.getMinOffset()));
            consumer.seek(tp, ack.getMinOffset());
        } else {
            logger.error(String.format("do not find key:%s of TopicPartition", key));
        }
    }

    private void seekOffset() {
        String strOffset = inner.logProcessorConf.getProperty(Constants.LOG_OFFSET);
        switch (strOffset) {
            case "none":
                break;
            case "begin":
                consumer.seekToBeginning(topicMap.get(inner.dbusDsConf.getTopic()));
                break;
            case "end":
                consumer.seekToEnd(topicMap.get(inner.dbusDsConf.getTopic()));
                break;
            default:
                for (String pair : StringUtils.split(strOffset, ",")) {
                    String[] val = StringUtils.split(pair, "->");
                    long offset = Long.parseLong(val[1]);
                    consumer.seek(topicPartitionMap.get(StringUtils.joinWith("_", inner.dbusDsConf.getTopic(), val[0])), offset);
                }
                break;
        }
        // 打印一下每个topic partition的消费位置
        if (topicMap.get(inner.dbusDsConf.getTopic()) != null) {
            for (TopicPartition partition : topicMap.get(inner.dbusDsConf.getTopic())) {
                long position = consumer.position(partition);
                logger.info(String.format("topic: %s, partition: %d, position: %d",
                        partition.topic(), partition.partition(), position));
            }
        }
        consumer.seekToEnd(topicMap.get(inner.dbusDsConf.getControlTopic()));
        inner.logProcessorConf.setProperty(Constants.LOG_OFFSET, "none");
        inner.zkHelper.saveLogProcessorConf(inner.logProcessorConf);
    }

    private void initConsumer() {
        kafkaConsumerConf = inner.zkHelper.loadKafkaConsumerConf();
        List<TopicPartition> assignTopics = new ArrayList<>();
        consumer = new KafkaConsumer<>(kafkaConsumerConf);
        assignTopics(consumer.partitionsFor(inner.dbusDsConf.getTopic()), assignTopics);
        assignTopics(consumer.partitionsFor(inner.dbusDsConf.getControlTopic()), assignTopics);
        if (assignTopics.isEmpty())
            throw new IllegalArgumentException("assign topic is empty!");
        consumer.assign(assignTopics);
    }

    private void processControlCommand(ConsumerRecord<String, byte[]> record) {
        try {
            String json = new String(record.value(), "utf-8");
            ControlType cmd = ControlType.getCommand(JSONObject.parseObject(json).getString("type"));
            switch (cmd) {
                case LOG_PROCESSOR_RELOAD_CONFIG: {
                    logger.info("LogProcessorSpout-{} 收到reload消息！Type: {}, Values: {} ", context.getThisTaskId(), cmd, json);
                    inner.close(true);
                    if (consumer != null) consumer.close();
                    init();
                    inner.zkHelper.saveReloadStatus(json, "LogProcessorSpout-" + context.getThisTaskId(), true);
                    break;
                }
                default:
                    break;
            }
        } catch (Exception ex) {
            logger.error("LogProcessorSpout processControlCommand():", ex);
            collector.reportError(ex);
        }
    }

    private void emitCtrlData(ConsumerRecord<String, byte[]> record) {
        AckMsg ackMsg = new AckMsg();
        ackMsg.setPartition(record.partition());
        ackMsg.setMaxOffset(record.offset());
        ackMsg.setTopic(inner.dbusDsConf.getControlTopic());
        this.collector.emit("ctrlOrHbStream", new Values(record, Constants.EMIT_DATA_TYPE_CTRL), ackMsg);
    }

    private void emitHbData(ConsumerRecord<String, byte[]> record) {
        AckMsg ackMsg = new AckMsg();
        ackMsg.setPartition(record.partition());
        ackMsg.setMinOffset(record.offset());
        ackMsg.setMaxOffset(record.offset());
        ackMsg.setTopic(inner.dbusDsConf.getTopic());
        this.collector.emit("ctrlOrHbStream", new Values(record, Constants.EMIT_DATA_TYPE_HEARTBEAT), ackMsg);
        emitCount++;
    }

    private void emitUmsHbData(ConsumerRecord<String, byte[]> record, String txTime, String host, String tableName, String umsSource) {
        AckMsg ackMsg = new AckMsg();
        ackMsg.setPartition(record.partition());
        ackMsg.setMinOffset(record.offset());
        ackMsg.setMaxOffset(record.offset());
        ackMsg.setTopic(inner.dbusDsConf.getTopic());
        Map<String, String> hbValue = new HashMap<>();
        hbValue.put("@timestamp", txTime);
        hbValue.put("partition", String.valueOf(record.partition()));
        hbValue.put("offset", String.valueOf(record.offset()));
        hbValue.put("clock", "");
        hbValue.put("type", "dbus-heartbeat");
        hbValue.put("host", host);
        hbValue.put("tableName", tableName);
        hbValue.put("umsSource", umsSource);
        this.collector.emit("ctrlOrHbStream", new Values(hbValue, Constants.EMIT_DATA_TYPE_HEARTBEAT), ackMsg);
        emitCount++;
    }

    private void emitFlumeHbData(ConsumerRecord<String, byte[]> record) {
        Map<String, String> hbValue = JSON.parseObject(record.value(), HashMap.class);
        hbValue.put("partition", String.valueOf(record.partition()));
        hbValue.put("offset", String.valueOf(record.offset()));
        AckMsg ackMsg = new AckMsg();
        ackMsg.setPartition(record.partition());
        ackMsg.setMinOffset(record.offset());
        ackMsg.setMaxOffset(record.offset());
        ackMsg.setTopic(inner.dbusDsConf.getTopic());
        this.collector.emit("ctrlOrHbStream", new Values(hbValue, Constants.EMIT_DATA_TYPE_HEARTBEAT), ackMsg);
        emitCount++;
    }

    private void emitFilebeatHbData(ConsumerRecord<String, byte[]> record) {
        String strValue = JSON.parseObject(new String(record.value())).getString("message");
        Map<String, String> hbValue = JSON.parseObject(strValue, HashMap.class);
        hbValue.put("partition", String.valueOf(record.partition()));
        hbValue.put("offset", String.valueOf(record.offset()));
        AckMsg ackMsg = new AckMsg();
        ackMsg.setPartition(record.partition());
        ackMsg.setMinOffset(record.offset());
        ackMsg.setMaxOffset(record.offset());
        ackMsg.setTopic(inner.dbusDsConf.getTopic());
        this.collector.emit("ctrlOrHbStream", new Values(hbValue, Constants.EMIT_DATA_TYPE_HEARTBEAT), ackMsg);
        emitCount++;
    }

    private void emitData(Map<Integer, List<ConsumerRecord<String, byte[]>>> partitionRecordMap) {
        for (Map.Entry<Integer, List<ConsumerRecord<String, byte[]>>> entry : partitionRecordMap.entrySet()) {
            AckMsg ackMsg = new AckMsg();
            ackMsg.setPartition(entry.getKey());
            ackMsg.setMinOffset(entry.getValue().get(0).offset());
            ackMsg.setMaxOffset(entry.getValue().get(entry.getValue().size() - 1).offset());
            ackMsg.setTopic(inner.dbusDsConf.getTopic());
            this.collector.emit("umsStream", new Values(entry.getValue(), Constants.EMIT_DATA_TYPE_NORMAL), ackMsg);
            emitCount++;
            if (logger.isDebugEnabled())
                logger.debug("spout emit ums data size:{}, emit total count:{}", entry.getValue().size(), emitCount);
        }
        partitionRecordMap.clear();
    }

    private class LogProcessorSpoutInner extends LogProcessorBase {
        public LogProcessorSpoutInner(Map conf) {
            super(conf);
        }
    }

}
