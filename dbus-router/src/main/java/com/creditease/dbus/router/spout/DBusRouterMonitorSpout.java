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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.PropertiesUtils;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.MonitorSpoutConfig;
import com.creditease.dbus.router.bean.Packet;
import com.creditease.dbus.router.bean.Sink;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mal on 2018/7/09.
 */
public class DBusRouterMonitorSpout extends BaseRichSpout {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterMonitorSpout.class);

    private TopologyContext context = null;
    private DBusRouterMonitorSpoutInner inner = null;

    private Set<String> urls = null;
    private MonitorSpoutConfig monitorSpoutConfig = new MonitorSpoutConfig();
    private Map<String, KafkaConsumer<String, byte[]>> consumerMap = new HashMap<>();

    private Map<String, Packet> cache = new HashMap<>();
    private Long baseFlushTime = 0L;
    private Long baseCommitTime = 0L;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.inner = new DBusRouterMonitorSpoutInner(conf);
            this.baseFlushTime = System.currentTimeMillis();
            this.baseCommitTime = System.currentTimeMillis();
            init();
            logger.info("monitor spout init completed.");
        } catch (Exception e) {
            logger.error("monitor spout init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            boolean isCtrl = false;
            Iterator<String> it = urls.iterator();
            boolean isReload = false;
            while (it.hasNext()) {
                KafkaConsumer<String, byte[]> consumer = consumerMap.get(it.next());
                ConsumerRecords<String, byte[]> records = consumer.poll(0);
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        if (isCtrlTopic(record.topic())) {
                            isCtrl = true;
                            isReload = processCtrlMsg(record);
                        } else if (isAssign(record.key())) {
                            isCtrl = false;
                            processData(record);
                        }
                        if (!isReload)
                            doAck(isCtrl, record.topic(), record.partition(), record.offset(), consumer);
                    } catch (Exception e) {
                        logger.error("process record error.", e);
                        if (!isReload)
                            doException(isCtrl, record.topic(), record.partition(), record.offset(), consumer);
                    }
                    if (isReload)
                        break;
                }
                if (isReload)
                    break;
            }
            if (isCtrl && !isReload)
                urls = new HashSet<>(consumerMap.keySet());
        }  catch (Exception e) {
            logger.error("monitor spout next tuple error.", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void close() {
        destroy();
        inner.close(false);
    }

    private void processData(ConsumerRecord<String, byte[]> record) {
        String key = record.key();
        String topic = record.topic();
        logger.info("topic:{}, key:{}", topic, key);

        if (StringUtils.isEmpty(key)) {
            logger.error("topic:{}, key:null, offset:{}", topic, record.offset());
            return;
        }

        // data_increment_heartbeat.mysql.mydb.cbm.t1#router_test_s_r5.6.0.0.1541041451552|1541041451550|ok.wh_placeholder
        String[] vals = StringUtils.split(key, ".");
        if (vals == null || vals.length != 10) {
            logger.error("receive heartbeat key is error. topic:{}, key:{}", topic, key);
            return;
        }

        long cpTime = 0L;
        long txTime = 0L;
        boolean isTableOK = true;

        if (StringUtils.contains(vals[8], "|")) {
            String times[] = StringUtils.split(vals[8], "|");
            cpTime = Long.valueOf(times[0]);
            txTime = Long.valueOf(times[1]);
            // 表明其实表已经abort了，但心跳数据仍然, 这种情况，只发送stat，不更新zk
            if (times.length == 3 && times[2].equals("abort")) {
                isTableOK = false;
                logger.warn("data abort. key:{}", key);
            }
        } else {
            isTableOK = false;
            logger.error("it should not be here. key:{}", key);
        }

        if (!isTableOK)
            return;

        String dsName = vals[2];
        if (StringUtils.contains(vals[2], "!")) {
            dsName = StringUtils.split(vals[2], "!")[0];
        } else {
            isTableOK = false;
            logger.error("it should not be here. key:{}", key);
        }
        String schemaName = vals[3];
        String tableName = vals[4];

        if (!isTableOK)
            return;

        // String dsPartition = vals[6];
        String ns = StringUtils.joinWith(".", dsName, schemaName, tableName);
        String path = StringUtils.joinWith("/", Constants.HEARTBEAT_PROJECT_MONITOR,
                inner.projectName, inner.topologyId, ns);

        // {"node":"/DBus/HeartBeat/ProjectMonitor/db4new/AMQUE/T_USER/0","time":1531180006336,"type":"checkpoint","txTime":1531180004040}
        Packet packet = new Packet();
        packet.setNode(path);
        packet.setType("checkpoint");
        packet.setTime(cpTime);
        packet.setTxTime(txTime);
        cache.put(path, packet);
        logger.info("put cache path:{}", path);

        if (isTimeUp(baseFlushTime)) {
            baseFlushTime = System.currentTimeMillis();
            flushCache();
        }
    }

    private boolean isTimeUp(long baseTime) {
        return (System.currentTimeMillis() - baseTime) > (1000 * 60);
    }

    private void flushCache() {
        Iterator<String> it = cache.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            serialize(key, cache.get(key));
        }
        cache.clear();
    }

    private void serialize(String path, Packet packet) {
        if (packet == null)
            return;
        String strJson = JSONObject.toJSONString(packet);
        byte[] data = strJson.getBytes(Charset.forName("UTF-8"));
        logger.info("serialize zk data path:{}, value:{}", path, strJson);
        inner.zkHelper.setData(path, data);
    }

    private void doException(boolean isCtrl,
                             String topic,
                             int partition,
                             long offset,
                             KafkaConsumer<String, byte[]> consumer) {
        if (isCtrl) {
            doAck(isCtrl, topic, partition, offset, consumer);
        } else {
            doFail(isCtrl, topic, partition, offset, consumer);
        }
    }

    private void doAck(boolean isCtrl,
                       String topic,
                       int partition,
                       long offset,
                       KafkaConsumer<String, byte[]> consumer) {

        boolean isCanCommit = isTimeUp(baseCommitTime);
        if (isCtrl) {
            isCanCommit = true;
        } else {
            if (isCanCommit) baseCommitTime = System.currentTimeMillis();
        }

        if (isCanCommit) {
            String key = StringUtils.joinWith("_", topic, String.valueOf(partition));
            TopicPartition tp = monitorSpoutConfig.getTopicPartitionMap().get(key);
            if (tp == null) {
                logger.warn(String.format("do not find key:%s of TopicPartition", key));
            } else {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(tp, new OffsetAndMetadata(offset));
                consumer.commitSync(offsets);
                logger.info("call consumer commitSync topic:{}, partition:{}, offset:{}", topic, partition, offset);
            }
        }
    }

    private void doFail(boolean isCtrl,
                        String topic,
                        int partition,
                        long offset,
                        KafkaConsumer<String, byte[]> consumer) {
        String key = StringUtils.joinWith("_", topic, String.valueOf(partition));
        TopicPartition tp = monitorSpoutConfig.getTopicPartitionMap().get(key);
        if (tp == null) {
            logger.warn(String.format("do not find key:%s of TopicPartition", key));
        } else {
            consumer.seek(tp, offset);
            logger.info("call consumer seek topic:{}, partition:{}, offset:{}", topic, partition, offset);
        }
    }

    private boolean processCtrlMsg(ConsumerRecord<String, byte[]> record) throws Exception {
        boolean isReload = false;
        String strCtrl = new String(record.value(), "UTF-8");
        logger.info("monitor spout process ctrl msg. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                isReload = true;
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
                startTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_STOP:
                stopTopologyTable(jsonCtrl);
                break;
            default:
                break;
        }
        return isReload;
    }

    private void reload(String ctrl) throws Exception {
        destroy();
        inner.close(true);
        init();
        inner.zkHelper.saveReloadStatus(ctrl, "DBusRouterMonitorSpout-" + context.getThisTaskId() , true);
        logger.info("monitor spout reload completed.");
    }

    private void startTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        List<Sink> sinks = inner.dbHelper.loadSinks(inner.topologyId, projectTopologyTableId);
        if (sinks != null && sinks.size() > 0) {
            boolean isUsing = inner.dbHelper.isUsingTopic(projectTopologyTableId);
            if (isUsing) {
                logger.info("table:{} of out put topic is using, don't need assign.", projectTopologyTableId);
            } else {
                if (monitorSpoutConfig.getSinks() == null) {
                    monitorSpoutConfig.setSinks(sinks);
                } else {
                    Sink sink = sinks.get(0);
                    monitorSpoutConfig.getSinks().add(sink);
                }
                monitorSpoutConfig.getTopicMap().clear();
                monitorSpoutConfig.getTopicPartitionMap().clear();
                initConsumer(true);
            }
        }
        logger.info("monitor spout start topology table:{} completed.", projectTopologyTableId);
    }

    private void stopTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        String dsName = payload.getString("dsName");
        String schemaName = payload.getString("schemaName");
        String tableName = payload.getString("tableName");
        Integer projectTopologyTableId = payload.getInteger("projectTopoTableId");
        String namespace = StringUtils.joinWith(".", dsName, schemaName, tableName);

        List<Sink> sinks = monitorSpoutConfig.getSinks();
        if (sinks != null) {
            Sink delSink = null;
            for (Sink sink : sinks) {
                String wkNs = StringUtils.joinWith(".", sink.getDsName(), sink.getSchemaName(), sink.getTableName());
                if (StringUtils.equals(wkNs, namespace)) {
                    delSink = sink;
                    break;
                }
            }
            if (delSink != null) {
                sinks.remove(delSink);
                boolean isUsing = inner.dbHelper.isUsingTopic(projectTopologyTableId);
                if (isUsing) {
                    logger.info("table:{} of out put topic is using, don't need remove.", projectTopologyTableId);
                } else {
                    monitorSpoutConfig.getTopicMap().clear();
                    monitorSpoutConfig.getTopicPartitionMap().clear();
                    initConsumer(true);
                }
            } else {
                logger.error("don't find name space:{} target sink, stop table fail.", namespace);
            }
        }
        logger.info("monitor spout stop topology table:{} completed.", projectTopologyTableId);
    }

    private boolean isAssign(String key) {
        if (StringUtils.isBlank(key))
            return false;
        String[] vals = StringUtils.split(key, ".");
        return StringUtils.equals("data_increment_heartbeat", vals[0]);
    }

    private void destroy() {
        for (Map.Entry<String, KafkaConsumer<String, byte[]>> entry : consumerMap.entrySet())
            if (entry.getValue() != null) entry.getValue().close();
        consumerMap.clear();
    }

    private String obtainCtrlTopic() {
        return StringUtils.joinWith("_", inner.topologyId, "ctrl");
    }

    private boolean isCtrlTopic(String topic) {
        return StringUtils.equals(topic, obtainCtrlTopic());
    }

    private void init() throws Exception {
        this.inner.init();
        this.monitorSpoutConfig.setSinks(inner.dbHelper.loadSinks(inner.topologyId));
        initConsumer(false);
    }

    private void initConsumer(boolean isCtrl) throws Exception {
        Map<String, Set<String>> urlTopicsMap = new HashMap<>();
        List<Sink> sinks = monitorSpoutConfig.getSinks();
        Properties consumerConf = inner.zkHelper.loadKafkaConsumerConf();

        String bootstrapServers = consumerConf.getProperty("bootstrap.servers");

        if (sinks != null && sinks.size() > 0) {
            for (Sink sink : sinks) {
                if (!consumerMap.containsKey(sink.getUrl())) {
                    Properties props = PropertiesUtils.copy(consumerConf);
                    props.setProperty("bootstrap.servers", sink.getUrl());
                    props.setProperty("group.id", StringUtils.joinWith("-", props.getProperty("group.id"), "monitor"));
                    props.setProperty("client.id", StringUtils.joinWith("-", props.getProperty("client.id"), "monitor"));
                    logger.info("monitor spout create consumer 1.");
                    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
                    consumerMap.put(sink.getUrl(), consumer);
                    Set<String> topics = new HashSet<>();
                    topics.add(sink.getTopic());
                    urlTopicsMap.put(sink.getUrl(), topics);
                } else {
                    if (urlTopicsMap.get(sink.getUrl()) == null) {
                        Set<String> topics = new HashSet<>();
                        topics.add(sink.getTopic());
                        urlTopicsMap.put(sink.getUrl(), topics);
                    } else {
                        urlTopicsMap.get(sink.getUrl()).add(sink.getTopic());
                    }
                }
            }

            logger.info("url topic map: {}", JSONObject.toJSONString(urlTopicsMap));

            for (Map.Entry<String, Set<String>> entry : urlTopicsMap.entrySet()) {
                List<TopicPartition> assignTopics = new ArrayList<>();
                KafkaConsumer<String, byte[]> consumer = consumerMap.get(entry.getKey());
                if (StringUtils.equals(entry.getKey(), bootstrapServers))
                    assignTopics(consumer.partitionsFor(obtainCtrlTopic()), assignTopics);
                for (String topic : entry.getValue())
                    assignTopics(consumer.partitionsFor(topic), assignTopics);
                consumer.assign(assignTopics);
                if (StringUtils.equals(entry.getKey(), bootstrapServers) && !isCtrl)
                    consumer.seekToEnd(monitorSpoutConfig.getTopicMap().get(obtainCtrlTopic()));
            }
        }

        if (!isCtrl && !consumerMap.containsKey(bootstrapServers)) {
            consumerConf.setProperty("group.id", StringUtils.joinWith("-", consumerConf.getProperty("group.id"), "monitor"));
            consumerConf.setProperty("client.id", StringUtils.joinWith("-", consumerConf.getProperty("client.id"), "monitor"));
            logger.info("monitor spout create consumer 2.");
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConf);
            consumerMap.put(bootstrapServers, consumer);
            List<TopicPartition> assignTopics = new ArrayList<>();
            assignTopics(consumer.partitionsFor(obtainCtrlTopic()), assignTopics);
            consumer.assign(assignTopics);
            consumer.seekToEnd(monitorSpoutConfig.getTopicMap().get(obtainCtrlTopic()));
        }

        // 当发生控制时，重新验证所需要的kafka url和已生成的consumer是否一致
        Set<String> urlsWk = new HashSet<>(consumerMap.keySet());
        for (String url : urlsWk) {
            if (StringUtils.equals(url, bootstrapServers))
                continue;
            if (!urlTopicsMap.containsKey(url)) {
                KafkaConsumer<String, byte[]> consumer = consumerMap.get(url);
                consumer.close();
                consumerMap.remove(url);
            }
        }

        if (!isCtrl)
            urls = new HashSet<>(consumerMap.keySet());

        logger.info("urls: {}", JSON.toJSONString(urls));
        logger.info("consumer map keys: {}", JSONObject.toJSONString(consumerMap.keySet()));
    }

    private void assignTopics(List<PartitionInfo> topicInfo, List<TopicPartition> assignTopics) {
        for (PartitionInfo pif : topicInfo) {
            TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
            assignTopics.add(tp);
            String key = StringUtils.joinWith("_", pif.topic(), String.valueOf(pif.partition()));
            monitorSpoutConfig.getTopicPartitionMap().put(key, tp);
            if (!monitorSpoutConfig.getTopicMap().containsKey(pif.topic())) {
                List<TopicPartition> partitions = new ArrayList<>();
                partitions.add(tp);
                monitorSpoutConfig.getTopicMap().put(pif.topic(), partitions);
            } else {
                monitorSpoutConfig.getTopicMap().get(pif.topic()).add(tp);
            }
        }
    }

    private class DBusRouterMonitorSpoutInner extends DBusRouterBase {
        public DBusRouterMonitorSpoutInner(Map conf) {
            super(conf);
        }
    }

}
