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

package com.creditease.dbus.router.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.PropertiesUtils;
import com.creditease.dbus.router.base.DBusRouterBase;
import com.creditease.dbus.router.bean.EmitWarp;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.util.DBusRouterConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mal on 2018/5/22.
 */
public class DBusRouterKafkaWriteBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterKafkaWriteBolt.class);

    private TopologyContext context = null;
    private OutputCollector collector = null;
    private DBusRouterKafkaWriteBoltInner inner = null;
    private Map<String, String> topicMap = new HashMap<>();
    private Map<String, String> sinksMap = new HashMap<>();
    private Properties kafkaProducerConf = null;
    private Map<String, KafkaProducer<String, byte[]>> producerMap = new HashMap<>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            inner = new DBusRouterKafkaWriteBoltInner(conf);
            init();
            logger.info("kafka write bolt init completed.");
        } catch (Exception e) {
            logger.error("kafka write bolt init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            EmitWarp<?> data = (EmitWarp<?>) input.getValueByField("data");
            if (data.isCtrl()) {
                String ctrl = (String) data.getData();
                processCtrlMsg(ctrl);
                collector.ack(input);
            } else if (data.isStat()) {
                String stat = (String) data.getData();
                String topic = inner.routerConfProps.getProperty(DBusRouterConstants.STAT_TOPIC, "dbus_statistic");
                String url = inner.routerConfProps.getProperty(DBusRouterConstants.STAT_URL);
                sendKafka(data.getKey(), stat, url, topic, data.getNameSpace(), input);
            } else if (data.isUMS() || data.isHB()) {
                String ums = (String) data.getData();
                String url = sinksMap.get(data.getNameSpace());
                String topic = topicMap.get(data.getNameSpace());
                sendKafka(data.getKey(), ums, url, topic, data.getNameSpace(), input);
            } else {
                logger.warn("not support process type. emit warp key: {}", data.getKey());
                collector.ack(input);
            }
        } catch (Exception e) {
            collector.fail(input);
            logger.error("kafka write bolt execute error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void cleanup() {
        destroy();
        inner.close(false);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private void processCtrlMsg(String strCtrl) throws Exception {
        logger.info("kafka write bolt process control message. cmd:{}", strCtrl);
        JSONObject jsonCtrl = JSONObject.parseObject(strCtrl);
        ControlType cmd = ControlType.getCommand(jsonCtrl.getString("type"));
        switch (cmd) {
            case ROUTER_RELOAD:
                reload(strCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_EFFECT:
                effectTopologyTable(jsonCtrl, false);
                break;
            case ROUTER_TOPOLOGY_TABLE_START:
                startTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_TABLE_STOP:
                stopTopologyTable(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_EFFECT:
                effectTopology(jsonCtrl);
                break;
            case ROUTER_TOPOLOGY_RERUN:
                rerunTopology(jsonCtrl);
                break;
            default:
                break;
        }
    }

    private void reload(String ctrl) throws Exception {
        destroy();
        inner.close(true);
        init();
        inner.zkHelper.saveReloadStatus(ctrl, "DbusRouterKafkaWriteBolt-" + context.getThisTaskId() , true);
        logger.info("kafka write bolt reload completed.");
    }

    private void effectTopology(JSONObject ctrl) throws Exception {
        inner.routerConfProps = inner.zkHelper.loadRouterConf();
        String url = inner.routerConfProps.getProperty(DBusRouterConstants.STAT_URL);
        if (!producerMap.containsKey(url)) {
            String clientId = StringUtils.joinWith("-", kafkaProducerConf.getProperty("client.id"), String.valueOf(producerMap.size() + 1));
            producerMap.put(url, obtainKafkaProducer(url, clientId));
        }
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopologyId = payload.getInteger("projectTopoId");
        inner.dbHelper.toggleProjectTopologyStatus(projectTopologyId, DBusRouterConstants.PROJECT_TOPOLOGY_STATUS_START);
        logger.info("kafka write bolt effect topology completed.");
    }

    private void effectTopologyTable(JSONObject ctrl, boolean isStart) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopoTableId = payload.getInteger("projectTopoTableId");
        List<Sink> sinkVos = inner.dbHelper.loadSinks(inner.topologyId, projectTopoTableId);
        if (sinkVos != null && sinkVos.size() > 0) {
            for (Sink vo : sinkVos) {
                String key = StringUtils.joinWith(".", vo.getDsName(), vo.getSchemaName(), vo.getTableName());
                topicMap.remove(key);
                topicMap.put(key, vo.getTopic());
                sinksMap.remove(key);
                String url = vo.getUrl();
                sinksMap.put(key, url);
                if (!producerMap.containsKey(url)) {
                    String clientId = StringUtils.joinWith("-", kafkaProducerConf.getProperty("client.id"), String.valueOf(producerMap.size() + 1));
                    producerMap.put(url, obtainKafkaProducer(url, clientId));
                }
                if (isStart) {
                    String path = StringUtils.joinWith("/", Constants.HEARTBEAT_PROJECT_MONITOR, vo.getProjectName(), inner.topologyId, key);
                    inner.zkHelper.createNode(path);
                }
            }
        }
        inner.dbHelper.toggleProjectTopologyTableStatus(projectTopoTableId, DBusRouterConstants.PROJECT_TOPOLOGY_TABLE_STATUS_START);
        logger.info("kafka write bolt effect topology table:{} completed.", projectTopoTableId);
    }

    private void startTopologyTable(JSONObject ctrl) throws Exception {
        effectTopologyTable(ctrl, true);
        JSONObject payload = ctrl.getJSONObject("payload");
        Integer projectTopoTableId = payload.getInteger("projectTopoTableId");
        inner.dbHelper.toggleProjectTopologyTableStatus(projectTopoTableId, DBusRouterConstants.PROJECT_TOPOLOGY_TABLE_STATUS_START);
        logger.info("kafka write bolt start topology table completed.");
    }

    private void stopTopologyTable(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        String dsName = payload.getString("dsName");
        String schemaName = payload.getString("schemaName");
        String tableName = payload.getString("tableName");
        String namespace = StringUtils.joinWith(".", dsName, schemaName, tableName);
        topicMap.remove(namespace);
        sinksMap.remove(namespace);

        Integer projectTopoTableId = payload.getInteger("projectTopoTableId");
        inner.dbHelper.toggleProjectTopologyTableStatus(projectTopoTableId, DBusRouterConstants.PROJECT_TOPOLOGY_TABLE_STATUS_STOP);

        String path = StringUtils.joinWith("/", Constants.HEARTBEAT_PROJECT_MONITOR, inner.projectName, inner.topologyId, namespace);
        inner.zkHelper.deleteNode(path);
        logger.info("kafka write bolt stop topology table:{} completed.", schemaName);
    }

    private void rerunTopology(JSONObject ctrl) throws Exception {
        JSONObject payload = ctrl.getJSONObject("payload");
        if (payload.containsKey("isFromEffect") && payload.getBoolean("isFromEffect")) {
            Integer projectTopoId = payload.getInteger("projectTopoId");
            String topologyConfig = inner.zkHelper.resetOffset();
            inner.dbHelper.updateProjectTopologyConfig(projectTopoId, topologyConfig);
        }
        logger.info("kafka write bolt rerun topology completed.");
    }

    private void sendKafka(String key, String data, String url, String topic, String ns, Tuple input) {
        if (StringUtils.isBlank(topic)) {
            logger.warn("namespace: {}, not obtain topic. ums: {}", ns, data);
            collector.fail(input);
            return;
        }
        KafkaProducer<String, byte[]> producer = producerMap.get(url);
        if (producer != null) {
            producer.send(new ProducerRecord<String, byte[]>(topic, key, data.getBytes()), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null)
                        collector.ack(input);
                    else
                        collector.fail(input);
                }
            });
        } else {
            collector.fail(input);
            logger.warn("namespace: {}, not obtain producer. sink:{} ums: {}", ns, url, data);
        }
    }

    private void destroy() {
        topicMap.clear();
        sinksMap.clear();
        for (Map.Entry<String, KafkaProducer<String, byte[]>> entry : producerMap.entrySet()) {
            KafkaProducer<String, byte[]> producer = entry.getValue();
            if (producer != null) producer.close();
        }
        producerMap.clear();
    }

    private void init() throws Exception {
        inner.init();
        kafkaProducerConf = inner.zkHelper.loadKafkaProducerConf();
        List<Sink> sinkVos = inner.dbHelper.loadSinks(inner.topologyId);
        if (sinkVos != null && sinkVos.size() > 0) {
            Set<String> urls = new HashSet<>();
            for (Sink vo : sinkVos) {
                String key = StringUtils.joinWith(".", vo.getDsName(), vo.getSchemaName(), vo.getTableName());
                topicMap.put(key, vo.getTopic());
                sinksMap.put(key, vo.getUrl());
                urls.add(vo.getUrl());
                String path = StringUtils.joinWith("/", Constants.HEARTBEAT_PROJECT_MONITOR, vo.getProjectName(), inner.topologyId, key);
                inner.zkHelper.createNode(path);
            }
            urls.add(inner.routerConfProps.getProperty(DBusRouterConstants.STAT_URL));
            int idx = 0;
            for (String url : urls) {
                String clientId = StringUtils.joinWith("-", kafkaProducerConf.getProperty("client.id"), String.valueOf(idx++));
                producerMap.put(url, obtainKafkaProducer(url, clientId));
            }
        } else {
            logger.warn("{} sink is empty.", inner.topologyId);
        }
    }

    private KafkaProducer<String, byte[]> obtainKafkaProducer(String url, String clientId) {
        KafkaProducer<String, byte[]> producer = null;
        try {
            Properties props = PropertiesUtils.copy(kafkaProducerConf);
            props.put("bootstrap.servers", url);
            props.put("client.id", clientId);


            producer = new KafkaProducer<>(props);
            logger.info("kafka write bolt create kafka producer. url:{}", url);
        } catch (Exception e) {
            logger.error("kafka write bolt create kafka producer error. url:{}", url);
        }
        return producer;
    }

    private class DBusRouterKafkaWriteBoltInner extends DBusRouterBase {
        public DBusRouterKafkaWriteBoltInner(Map conf) {
            super(conf);
        }
    }

}
