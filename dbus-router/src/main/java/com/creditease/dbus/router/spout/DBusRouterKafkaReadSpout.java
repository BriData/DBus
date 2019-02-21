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

package com.creditease.dbus.router.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.Ack;
import com.creditease.dbus.router.bean.EmitWarp;
import com.creditease.dbus.router.bean.ReadSpoutConfig;
import com.creditease.dbus.router.bean.Resources;
import com.creditease.dbus.router.spout.ack.AckCallBack;
import com.creditease.dbus.router.spout.ack.AckWindows;
import com.creditease.dbus.router.util.DBusRouterConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
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

/**
 * Created by mal on 2018/5/22.
 */
public class DBusRouterKafkaReadSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterKafkaReadSpout.class);

    private TopologyContext context = null;
    private SpoutOutputCollector collector = null;
    private DBusRouterKafkaReadSpoutInner inner = null;

    private ReadSpoutConfig readSpoutConfig = null;
    private KafkaConsumer<String, byte[]> consumer = null;
    private AckWindows ackWindows = null;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            this.inner = new DBusRouterKafkaReadSpoutInner(conf);
            init();
            logger.info("spout init completed.");
        } catch (Exception e) {
            logger.error("spout init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            for (ConsumerRecord<String, byte[]> record : records) {
                if (isCtrlTopic(record.topic())) {
                    processCtrlMsg(record);
                    emitCtrlData(record);
                } else if (isAssign(record.key())) {
                    emitUmsData(record);
                }
            }
        } catch (Exception e) {
            logger.error("spout next tuple error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void ack(Object msgId) {
        ackWindows.ack((Ack) msgId);
    }

    @Override
    public void fail(Object msgId) {
        ackWindows.fail((Ack) msgId);
    }

    @Override
    public void close() {
        destroy();
        ackWindows.clear();
        ackWindows = null;
        inner.close(false);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("umsOrHbStream", new Fields("data", "ns"));
        declarer.declareStream("ctrlStream", new Fields("data"));
    }

    private void processCtrlMsg(ConsumerRecord<String, byte[]> record) throws Exception {
        String strCtrl = new String(record.value(), "UTF-8");
        logger.info("spout process ctrl msg. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
                startTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_STOP:
                stopTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_RERUN:
                rerunTopology(jsonCtrl);
                break;
            default:
                break;
        }
    }

    private void rerunTopology(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        String offset = payload.getString("offset");
        if (StringUtils.isNotBlank(offset)) {
            ackWindows.flush();
            ackWindows.clear();
            seekOffset(payload.getString("offset"));
        }
        logger.info("spout rerun topology completed.");
    }

    private void reload(String ctrl) throws Exception {
        destroy();
        inner.close(true);
        init();
        inner.zkHelper.saveReloadStatus(ctrl, "DbusRouterKafkaReadSpout-" + context.getThisTaskId() , true);
        logger.info("spout reload completed.");
    }

    private boolean isAlreadyAssign(String topic) {
        boolean isAssign = false;
        Set<TopicPartition> topics = consumer.assignment();
        for (TopicPartition tp : topics) {
            if (StringUtils.equals(tp.topic(), topic)) {
                isAssign = true;
                break;
            }
        }
        return isAssign;
    }

    private void startTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        List<Resources> resources = inner.dbHelper.loadResources(inner.topologyId, projectTopologyTableId);
        if (resources != null && resources.size() > 0) {
            Resources vo = resources.get(0);
            String namespace = StringUtils.joinWith(".", vo.getDsName(), vo.getSchemaName(), vo.getTableName());
            readSpoutConfig.getTopics().add(vo.getTopicName());
            readSpoutConfig.getNamespaces().add(namespace);
            readSpoutConfig.getNamespaceTableIdPair().put(namespace, vo.getTableId());
            if (!isAlreadyAssign(vo.getTopicName())) {
                readSpoutConfig.getTopicPartitionMap().clear();
                readSpoutConfig.getTopicMap().clear();
                initConsumer(true);
            }
            ackWindows.flush();
            ackWindows.clear();
            if (StringUtils.isNotBlank(payload.getString("offset")))
                seekOffset(payload.getString("offset"));
        }
        logger.info("spout start topology table id:{}, table name:{} completed.",
                projectTopologyTableId, payload.getString("tableName"));
    }

    private void stopTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        String dsName = payload.getString("dsName");
        String schemaName = payload.getString("schemaName");
        String tableName = payload.getString("tableName");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");

        String namespace = StringUtils.joinWith(".", dsName, schemaName, tableName);
        readSpoutConfig.getNamespaces().remove(namespace);
        readSpoutConfig.getNamespaceTableIdPair().remove(namespace);

        List<Resources> resources = inner.dbHelper.loadResources(inner.topologyId, projectTopologyTableId);
        if (resources != null && resources.size() > 0) {
            Resources vo = resources.get(0);
            if (!inner.dbHelper.isUsingTopic(projectTopologyTableId)) {
                readSpoutConfig.getTopics().remove(vo.getTopicName());
                initConsumer(true);
            }
            ackWindows.flush();
            ackWindows.clear();
        }

        logger.info("spout stop topology table id:{}, table name:{} completed.",
                projectTopologyTableId, payload.getString("tableName"));
    }

    private void destroy() {
        if (consumer != null) consumer.close();
    }

    private void emitCtrlData(ConsumerRecord<String, byte[]> record) {
        Ack ackVo = obtainAck(record);
        ackWindows.add(ackVo);
        this.collector.emit("ctrlStream", new Values(obtainEmitWarp(record, "ctrl", null)), ackVo);
    }

    private void emitUmsData(ConsumerRecord<String, byte[]> record) {
        String[] vals = StringUtils.split(record.key(), ".");
        String nameSpace = StringUtils.joinWith(".", vals[2], vals[3], vals[4]);
        Ack ackVo = obtainAck(record);
        ackWindows.add(ackVo);
        this.collector.emit("umsOrHbStream", new Values(obtainEmitWarp(record, record.key(), nameSpace), nameSpace), ackVo);
    }

    private Ack obtainAck(ConsumerRecord<String, byte[]> record) {
        Ack ack = new Ack();
        ack.setTopic(record.topic());
        ack.setPartition(record.partition());
        ack.setOffset(record.offset());
        return ack;
    }

    private EmitWarp<ConsumerRecord<String, byte[]>> obtainEmitWarp(ConsumerRecord<String, byte[]> record, String key, String namespace) {
        String tempKey = key;
        if (!StringUtils.equals("ctrl", key)) {
            // eg. data_increment_heartbeat.oracle.db4_3.AMQUE.T_CONTACT_INFO.3.0.0.1531709399507|1531709398879|ok.wh_placeholder
            //     data_increment_data.oracle.db4_3.AMQUE.T_CONTACT_INFO.3.0.0.1531709399889.wh_placeholder
            // String[] arr = ArrayUtils.insert(5, StringUtils.split(key, "."), inner.topologyId);
            // tempKey = StringUtils.joinWith(".", arr);
            String[] arr = StringUtils.split(key, ".");
            arr[2] = StringUtils.joinWith("!", arr[2], inner.topologyId);
            tempKey = StringUtils.joinWith(".", arr);
        }
        EmitWarp<ConsumerRecord<String, byte[]>> data = new EmitWarp<>(tempKey);
        data.setData(record);
        data.setTableId(readSpoutConfig.getNamespaceTableIdPair().get(namespace));
        return data;
    }

    private boolean isAssign(String key) {
        String[] vals = StringUtils.split(key, ".");
        Set<String> nameSpaces = readSpoutConfig.getNamespaces();
        return nameSpaces.contains(StringUtils.joinWith(".", vals[2], vals[3], vals[4]));
    }

    private String obtainCtrlTopic() {
        return StringUtils.joinWith("_", inner.topologyId, "ctrl");
    }

    private boolean isCtrlTopic(String topic) {
        return StringUtils.equals(topic, obtainCtrlTopic());
    }

    private void init() throws Exception {
        this.inner.init();
        this.readSpoutConfig = inner.dbHelper.loadReadSpoutConfig(inner.topologyId);
        initAckWindows();
        initConsumer(false);
        seekOffset(inner.routerConfProps.getProperty(DBusRouterConstants.TOPIC_OFFSET));
        consumer.seekToEnd(readSpoutConfig.getTopicMap().get(obtainCtrlTopic()));
        inner.routerConfProps.setProperty(DBusRouterConstants.TOPIC_OFFSET, "none");
        inner.zkHelper.resetOffset();
    }

    private void initAckWindows() {
        this.ackWindows = new AckWindows(1000, new AckCallBack() {
            @Override
            public void ack(Ack ackVo) {
                String key = StringUtils.joinWith("_", ackVo.getTopic(), String.valueOf(ackVo.getPartition()));
                TopicPartition tp = readSpoutConfig.getTopicPartitionMap().get(key);
                if (tp != null) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(tp, new OffsetAndMetadata(ackVo.getOffset()));
                    logger.info("call consumer commitSync topic:{}, partition:{}, offset:{}",
                            ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
                    consumer.commitSync(offsets);
                } else {
                    logger.warn(String.format("do not find key:%s of TopicPartition", key));
                }
            }
            @Override
            public void fail(Ack ackVo) {
                String key = StringUtils.joinWith("_", ackVo.getTopic(), String.valueOf(ackVo.getPartition()));
                TopicPartition tp = readSpoutConfig.getTopicPartitionMap().get(key);
                if (tp != null) {
                    logger.info("call consumer seek topic:{}, partition:{}, offset:{}",
                            ackVo.getTopic(), ackVo.getPartition(), ackVo.getOffset());
                    consumer.seek(tp, ackVo.getOffset());
                } else {
                    logger.warn(String.format("do not find key:%s of TopicPartition", key));
                }
            }
        });
    }

    private void initConsumer(boolean isCtrl) throws Exception {
        List<TopicPartition> assignTopics = new ArrayList<>();
        if (!isCtrl) {
            Properties props = inner.zkHelper.loadKafkaConsumerConf();
            logger.info("read spout create consumer.");
            consumer = new KafkaConsumer<>(props);
        }
        assignTopics(consumer.partitionsFor(obtainCtrlTopic()), assignTopics);
        for (String topic : readSpoutConfig.getTopics())
            assignTopics(consumer.partitionsFor(topic), assignTopics);
        consumer.assign(assignTopics);
    }

    private void assignTopics(List<PartitionInfo> topicInfo, List<TopicPartition> assignTopics) {
        for (PartitionInfo pif : topicInfo) {
            TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
            assignTopics.add(tp);
            String key = StringUtils.joinWith("_", pif.topic(), String.valueOf(pif.partition()));
            readSpoutConfig.getTopicPartitionMap().put(key, tp);
            if (!readSpoutConfig.getTopicMap().containsKey(pif.topic())) {
                List<TopicPartition> partitions = new ArrayList<>();
                partitions.add(tp);
                readSpoutConfig.getTopicMap().put(pif.topic(), partitions);
            } else {
                readSpoutConfig.getTopicMap().get(pif.topic()).add(tp);
            }
        }
    }

    private void seekOffset(String strOffset) throws Exception {
        Set<String> topics = readSpoutConfig.getTopics();
        switch (strOffset) {
            case "none":
                break;
            case "begin":
                    for (String topic : topics)
                        if(readSpoutConfig.getTopicMap().get(topic) != null)
                            consumer.seekToBeginning(readSpoutConfig.getTopicMap().get(topic));
                break;
            case "end":
                    for (String topic : topics)
                        if (readSpoutConfig.getTopicMap().get(topic) != null)
                            consumer.seekToEnd(readSpoutConfig.getTopicMap().get(topic));
                break;
            default:
                JSONArray jsonArray = JSONArray.parseArray(strOffset);
                for (int i=0; i<jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    String topic = jsonObject.getString("topic");
                    String offsetParis = jsonObject.getString("offsetParis");
                    if (StringUtils.contains(offsetParis, "->")) {
                        for (String pair : StringUtils.split(offsetParis, ",")) {
                            String[] val = StringUtils.split(pair, "->");
                            String key = StringUtils.joinWith("_", topic, val[0]);
                            long offset = -1;
                            if (val.length == 2) {
                                offset = Long.parseLong(val[1]);
                            }
                            if (readSpoutConfig.getTopicPartitionMap().get(key) !=  null) {
                                if (offset != -1) {
                                    logger.info("topic:{}, partition:{}, seek position:{}", topic, val[0], offset);
                                    consumer.seek(readSpoutConfig.getTopicPartitionMap().get(key), offset);
                                } else {
                                    logger.info("topic:{}, partition:{}, not seek position", topic, val[0]);
                                }
                                long position = consumer.position(readSpoutConfig.getTopicPartitionMap().get(key));
                                logger.info("topic:{}, partition:{}, start position:{}", topic, val[0], position);
                            } else {
                                logger.warn("don't find topic:{}, partition:{} of info.", topic, val[0]);
                            }
                        }
                    } else {
                        if (StringUtils.equals(offsetParis, "begin")) {
                            consumer.seekToBeginning(readSpoutConfig.getTopicMap().get(topic));
                        } else if (StringUtils.equals(offsetParis, "end")) {
                            consumer.seekToEnd(readSpoutConfig.getTopicMap().get(topic));
                        }
                    }
                }
                break;
        }
    }

    private class DBusRouterKafkaReadSpoutInner extends DBusRouterBase {
        public DBusRouterKafkaReadSpoutInner(Map conf) {
            super(conf);
        }
    }

}
