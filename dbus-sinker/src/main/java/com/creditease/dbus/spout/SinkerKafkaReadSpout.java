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


package com.creditease.dbus.spout;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.spout.queue.EmitDataList;
import com.creditease.dbus.spout.queue.EmitDataListManager;
import com.creditease.dbus.spout.queue.MessageStatusQueueManager;
import com.creditease.dbus.tools.SinkerBaseMap;
import com.creditease.dbus.tools.SinkerConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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

public class SinkerKafkaReadSpout extends BaseRichSpout {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private SpoutOutputCollector collector = null;
    private TopologyContext context = null;
    private KafkaConsumer<String, byte[]> consumer = null;
    private KafkaConsumer<String, byte[]> ctrlConsumer = null;
    private SinkerBaseMap inner = null;
    private MessageStatusQueueManager msgQueueMgr = null;
    private EmitDataListManager emitDataListManager = null;
    private Map<String, DBusConsumerRecord<String, byte[]>> reloadCtrlMsg = null;
    // 保存所有topic,partion的commit时间,用于处理定时commit,commit太频繁kafka会报错
    private Map<String, Long> commitInfoMap = null;
    private int MAX_FLOW_THRESHOLD = 0;
    private int flowBytes = 0;
    private int rCount = 0;
    private int COMMIT_INTERVALS = 0;
    private long lastCommitTime = 0;
    // 处理commit,emit,reload动作间隔时间,不要太频繁
    private int MANAGE_INTERVALS = 0;
    private long lastEmitTime = 0;
    private long lastCheckReloadTime = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            this.inner = new SinkerBaseMap(conf);
            this.reloadCtrlMsg = new HashMap<>();
            init(null);
            logger.info("[kafka read spout] init completed.");
        } catch (Exception e) {
            logger.error("[kafka read spout] init error.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            // commit offset to kafka
            commitOffsetToKafka();
            // emit data to bolt
            emitDataToBolt();
            // 处理reload消息
            reloadComplete();
            // 如果读取的流量过大则要sleep一下
            if (flowLimitation()) return;

            ConsumerRecords<String, byte[]> ctrlRecords = ctrlConsumer.poll(0);
            for (ConsumerRecord<String, byte[]> record : ctrlRecords) {
                logger.info("[kafka read spout] received reload message .{} ", new String(record.value(), "utf-8"));
                DBusConsumerRecord<String, byte[]> consumerRecord = new DBusConsumerRecord(record);
                processCtrlMsg(consumerRecord);
            }

            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            if (records.isEmpty()) {
                bubble();
                return;
            }
            for (ConsumerRecord<String, byte[]> record : records) {
                logger.debug("[received] topic: {}, offset: {}, key: {}, size:{}", record.topic(), record.offset(), record.key(), record.serializedValueSize());
                DBusConsumerRecord<String, byte[]> consumerRecord = new DBusConsumerRecord(record);
                String key = bildNSKey(consumerRecord);
                if (!inner.tableNamespaces.contains(key)) {
                    logger.debug("[ignore] topic: {}, offset: {}, key: {}, size:{}", record.topic(), record.offset(), record.key(), record.serializedValueSize());
                    continue;
                }
                msgQueueMgr.addMessage(consumerRecord);
                emitDataListManager.emit(key, consumerRecord);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void emitDataToBolt() {
        // 如果存在未处理的reload消息
        if ((System.currentTimeMillis() - lastEmitTime) < MANAGE_INTERVALS) return;
        List<EmitDataList> emttDataList = null;
        if (!reloadCtrlMsg.isEmpty()) {
            // 如果有未处理的ctr消息,丢弃当前读取到全部数据
            // 这里发现如果进行了加减topic,会导致之前的拓扑分组发生变化,导致出现数据ack到了另外的spout,会出现无限等待的问题
            // 处理方法:简单粗暴,直接丢弃当前全部数据
            emitDataListManager.removeAll();
            msgQueueMgr.removeAll();
        } else {
            emttDataList = emitDataListManager.getEmitDataList();
        }
        emit(emttDataList);
        lastEmitTime = System.currentTimeMillis();
    }

    private void emit(List<EmitDataList> emttDataList) {
        if (emttDataList != null && !emttDataList.isEmpty()) {
            emttDataList.forEach(emitDataList -> {
                DBusConsumerRecord<String, byte[]> consumerRecord = emitDataList.getDataList().get(0);
                String key = bildNSKey(consumerRecord);
                collector.emit("dataStream", new Values(emitDataList.getDataList(), key), emitDataList.getDataList());
                if (logger.isDebugEnabled()) {
                    emitDataList.getDataList().forEach(record -> {
                        logger.debug("[emit] topic: {}, offset: {}, key: {}, size:{}", record.topic(), record.offset(), key, emitDataList.getSize());
                    });
                }
                flowBytes += emitDataList.getSize();
                emitDataListManager.remove(key);
            });
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId != null && List.class.isInstance(msgId)) {
            List<DBusConsumerRecord<String, byte[]>> recordList = getMessageId(msgId);
            recordList.forEach(record -> {
                reduceFlowSize(record.serializedValueSize());
                // 标记处理成功
                msgQueueMgr.okAndGetCommitPoint(record);
                logger.info("[ack] topic: {}, offset: {}, key: {}, size:{}", record.topic(), record.offset(), record.key(), record.serializedValueSize());
            });
        } else {
            logger.info("[ack] receive ack message[{}]", msgId.getClass().getName());
        }
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        if (msgId != null && List.class.isInstance(msgId)) {
            List<DBusConsumerRecord<String, byte[]>> recordList = getMessageId(msgId);
            recordList.forEach(record -> {
                logger.error("[fail] topic: {}, offset: {}, key: {}, size: {}", record.topic(), record.offset(), record.key(), record.serializedValueSize());
                reduceFlowSize(record.serializedValueSize());
                // 标记处理失败,获取seek点
                DBusConsumerRecord<String, byte[]> seekPoint = msgQueueMgr.failAndGetSeekPoint(record);
                if (seekPoint != null) {
                    logger.warn("[fail] seek topic {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset());
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                }
            });
        } else {
            logger.warn("[fail] receive fail message[{}]", msgId.getClass().getName());
        }
        super.fail(msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("data", "ns"));
        declarer.declareStream("ctrlStream", new Fields("data"));
    }

    /**
     * 每隔固定毫秒数commit一次
     */
    private void commitOffsetToKafka() {
        if ((System.currentTimeMillis() - lastCommitTime) < MANAGE_INTERVALS) return;
        commitInfoMap.entrySet().forEach(entry -> {
            if ((System.currentTimeMillis() - entry.getValue()) >= COMMIT_INTERVALS) {
                DBusConsumerRecord<String, byte[]> record = msgQueueMgr.getCommitPoint(entry.getKey());
                if (record != null) {
                    logger.info("[commit] commit async {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset() + 1);
                    final int partition = record.partition();
                    final long offset = record.offset();
                    Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, ""));
                    consumer.commitAsync(map, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("[commit] commit error [topic:{},partition:{},offset:{}]., error message: {}", record.topic(), partition, offset, exception.getCause().getMessage());
                        }
                    });
                    msgQueueMgr.committed(record);
                }
                commitInfoMap.put(entry.getKey(), System.currentTimeMillis());
            }
        });
        lastCommitTime = System.currentTimeMillis();
    }

    private void init(JSONArray topicInfos) throws Exception {
        inner.init();
        this.msgQueueMgr = new MessageStatusQueueManager();
        this.MAX_FLOW_THRESHOLD = Integer.parseInt(inner.sinkerConfProps.getProperty(SinkerConstants.SPOUT_MAX_FLOW_THRESHOLD));
        this.COMMIT_INTERVALS = Integer.parseInt(inner.sinkerConfProps.getProperty(SinkerConstants.COMMIT_INTERVALS));
        this.MANAGE_INTERVALS = Integer.parseInt(inner.sinkerConfProps.getProperty(SinkerConstants.MANAGE_INTERVALS));
        Integer emitSize = Integer.parseInt(inner.sinkerConfProps.getProperty(SinkerConstants.EMIT_SIZ));
        Integer emitIntervals = Integer.parseInt(inner.sinkerConfProps.getProperty(SinkerConstants.EMIT_INTERVALS));
        //该对象不参与reload
        if (this.emitDataListManager == null) {
            this.emitDataListManager = new EmitDataListManager(emitSize, emitIntervals);
        }
        initConsumer(topicInfos);
    }

    private void destroy() {
        logger.info("[kafka read spout] reload close consumer");
        try {
            if (consumer != null) consumer.close();
            if (ctrlConsumer != null) ctrlConsumer.close();
            inner.close();
        } catch (Exception e) {
            logger.error("[kafka read spout] destroy resource error {}", e.getMessage(), e);
        }
    }

    private void initConsumer(JSONArray topicInfos) throws Exception {
        // 初始化数据consumer
        int taskIndex = context.getThisTaskIndex();
        int spoutSize = context.getComponentTasks(context.getThisComponentId()).size();
        List<String> allTopics = inner.sourceTopics;
        List<String> topicList = getResultTopics(allTopics, spoutSize).get(taskIndex);
        logger.info("[kafka read spout] will init consumer with task index: {}, spout size: {}, topics: {} ", taskIndex, spoutSize, topicList);
        Properties properties = inner.zkHelper.loadSinkerConf(SinkerConstants.CONSUMER);
        properties.put("client.id", inner.sinkerName + "SinkerKafkaReadClient_" + taskIndex);
        this.consumer = new KafkaConsumer<>(properties);
        Map<String, List<PartitionInfo>> topicsMap = consumer.listTopics();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicList.forEach(topic -> {
            List<PartitionInfo> partitionInfos = topicsMap.get(topic);
            if (partitionInfos != null && !partitionInfos.isEmpty()) {
                partitionInfos.forEach(partitionInfo -> topicPartitions.add(new TopicPartition(topic, partitionInfo.partition())));
            } else {
                topicPartitions.add(new TopicPartition(topic, 0));
            }
        });
        consumer.assign(topicPartitions);
        if (topicInfos != null) {
            topicInfos.forEach(info -> {
                JSONObject topicInfo = (JSONObject) info;
                if (topicList.contains(topicInfo.getString("topic"))) {
                    TopicPartition topicPartition = new TopicPartition(topicInfo.getString("topic"), topicInfo.getInteger("partition"));
                    consumer.seek(topicPartition, topicInfo.getLong("position"));
                }
            });
        }
        logger.info("[kafka read spout] init consumer success with task index: {}, spout size: {}, topicPartitions: {} ", taskIndex, spoutSize, topicPartitions);

        // 初始化 commitInfoMap
        this.commitInfoMap = new HashMap<>();
        topicPartitions.forEach(topicPartition -> {
            commitInfoMap.put(topicPartition.topic() + "-" + topicPartition.partition(), System.currentTimeMillis());
        });

        // 初始化ctrl consumer
        logger.info("[kafka read spout] will init ctrl consumer with task index: {},topic: {} ", taskIndex, getCtrlTopic());
        properties.put("group.id", inner.sinkerName + "SinkerCtrlGroup_" + taskIndex);
        properties.put("client.id", inner.sinkerName + "SinkerCtrlClient_" + taskIndex);
        //ctrl topic不跟踪消息处理
        properties.put("enable.auto.commit", true);
        this.ctrlConsumer = new KafkaConsumer<>(properties);
        List<TopicPartition> ctrlTopicPartitions = Collections.singletonList(new TopicPartition(getCtrlTopic(), 0));
        ctrlConsumer.assign(ctrlTopicPartitions);
        ctrlConsumer.seekToEnd(Collections.singletonList(new TopicPartition(getCtrlTopic(), 0)));
        logger.info("[kafka read spout] init ctrl consumer success with task index: {}, topicPartitions: {} ", taskIndex, ctrlTopicPartitions);
    }

    public List<List<String>> getResultTopics(List<String> allTopics, int n) {
        List<List<String>> result = new ArrayList<>();
        int remainder = allTopics.size() % n;  //(先计算出余数)
        int number = allTopics.size() / n;  //然后是商
        int offset = 0;//偏移量
        for (int i = 0; i < n; i++) {
            List<String> value = null;
            if (remainder > 0) {
                value = allTopics.subList(i * number + offset, (i + 1) * number + offset + 1);
                remainder--;
                offset++;
            } else {
                value = allTopics.subList(i * number + offset, (i + 1) * number + offset);
            }
            result.add(value);
        }
        return result;
    }

    private void reloadComplete() {
        if (!reloadCtrlMsg.isEmpty()) {
            // 重新加载配置文件
            reload();
        }
    }

    private boolean flowLimitation() {
        if (flowBytes >= MAX_FLOW_THRESHOLD) {
            logger.info("[kafka read spout] flow control: Spout gets {} bytes data.", flowBytes);
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            return true;
        }
        return false;
    }

    public void reduceFlowSize(int recordSize) {
        this.flowBytes -= recordSize;
        logger.debug("[kafka read spout] reduce flow size:{}", this.flowBytes);
    }

    private void bubble() {
        rCount++;
        if (rCount == 10000) {
            logger.info("[kafka read spout] running...");
            rCount = 0;
            flowBytes = 0; // 容错,避免出现数据处理完成或者失败,没有成功的减少flowBytes数的情况导致实际流量变小的问题
        }
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private void processCtrlMsg(DBusConsumerRecord<String, byte[]> record) throws Exception {
        String strCtrl = new String(record.value(), "UTF-8");
        JSONObject json = JSONObject.parseObject(strCtrl);
        String type = json.getString("type");
        if (reloadCtrlMsg.containsKey(type)) {
            logger.info("[kafka read spout] ignore duplicate ctrl messages . {}", json);
            return;
        }
        reloadCtrlMsg.put(type, record);
    }

    private void reload() {
        try {
            for (Map.Entry<String, DBusConsumerRecord<String, byte[]>> entry : reloadCtrlMsg.entrySet()) {
                String strCtrl = new String(entry.getValue().value(), "UTF-8");
                logger.info("[kafka read spout] reload. cmd:{}", strCtrl);
                JSONObject json = JSONObject.parseObject(strCtrl);
                // 拖回重跑不提交到bolt,只在spout处理
                if (entry.getKey().equals(ControlType.SINKER_DRAG_BACK_RUN_AGAIN.name())) {
                    destroy();
                    init(json.getJSONObject("payload").getJSONArray("offset"));
                }
                if (entry.getKey().equals(ControlType.SINKER_RELOAD_CONFIG.name())) {
                    destroy();
                    init(null);
                    collector.emit("ctrlStream", new Values(Collections.singletonList(entry.getValue())));
                }
                inner.zkHelper.saveReloadStatus(new String(entry.getValue().value(), "utf-8"), "SinkerKafkaReadSpout-" + context.getThisTaskId(), true);
            }
            this.reloadCtrlMsg.clear();
            logger.info("[kafka read spout] reload completed.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            this.reloadCtrlMsg.clear();
        }
    }

    private String bildNSKey(DBusConsumerRecord<String, byte[]> record) {
        String[] vals = StringUtils.split(record.key(), ".");
        return StringUtils.joinWith(".", vals[2], vals[3], vals[4]);
    }

    private String getCtrlTopic() {
        return inner.sinkerName + "_sinker_ctrl";
    }

    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }

}
