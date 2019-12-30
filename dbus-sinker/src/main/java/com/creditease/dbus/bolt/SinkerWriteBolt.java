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


package com.creditease.dbus.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.bolt.stat.MessageStatManger;
import com.creditease.dbus.bolt.stat.Stat;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.handler.SinkerHdfsWritehandler;
import com.creditease.dbus.handler.SinkerWriteHandler;
import com.creditease.dbus.tools.SinkerBaseMap;
import com.creditease.dbus.tools.SinkerConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SinkerWriteBolt extends BaseRichBolt {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private SinkerBaseMap inner = null;
    private OutputCollector collector = null;
    private TopologyContext context = null;
    private KafkaProducer<String, byte[]> producer = null;
    private long ctrlMsgId = 0;
    private MessageStatManger statManger = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            this.context = context;
            this.collector = collector;
            inner = new SinkerBaseMap(conf);
            init();
            logger.info("[write bolt] init completed.");
        } catch (Exception e) {
            logger.error("[write bolt] init error.", e);
            collector.reportError(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        List<DBusConsumerRecord<String, byte[]>> data = (List<DBusConsumerRecord<String, byte[]>>) input.getValueByField("data");
        try {
            if (isCtrl(data)) {
                JSONObject json = JSON.parseObject(new String(data.get(0).value(), "utf-8"));
                logger.info("[write bolt] received reload message .{} ", json);
                Long id = json.getLong("id");
                if (ctrlMsgId == id) {
                    logger.info("[write bolt] ignore duplicate ctrl messages . {}", json);
                } else {
                    ctrlMsgId = id;
                    processCtrlMsg(json);
                }
            } else {
                sendData(data);
            }
            collector.ack(input);
        } catch (Exception e) {
            logger.error("[write bolt] execute error.", e);
            collector.fail(input);
        }
    }

    private void sendData(List<DBusConsumerRecord<String, byte[]>> data) throws Exception {
        long start = System.currentTimeMillis();
        DBusConsumerRecord<String, byte[]> record = data.get(0);
        String statKey = getStatKey(StringUtils.split(record.key(), "."));
        int totalCnt = 0;
        SinkerWriteHandler sinkerWriteHandler = getSinkerWriteHandler();
        // 为数据添加换行符
        StringBuilder sb = new StringBuilder();
        for (DBusConsumerRecord<String, byte[]> consumerRecord : data) {
            // heartbeat消息不进hdfs
            if (isHeartBeat(consumerRecord)) {
                sendStatAndHeartBeat(consumerRecord);
            } else if (isUMS(consumerRecord)) {
                String str = new String(consumerRecord.value(), "utf-8") + "\n";
                int cnt = countDataRows(str);
                totalCnt += cnt;
                statManger.put(statKey, record.key(), cnt);
                logger.debug("[count] {},{},{}", statKey, cnt, statManger.get(statKey).getSuccessCnt());
                sb.append(str);
            } else {
                logger.warn("[write bolt] not support process type. emit warp key: {}", data);
            }
        }

        if (totalCnt != 0) {
            try {
                sinkerWriteHandler.sendData(inner, record, sb.toString());
                logger.info("[write hdfs] topic: {}, offset: {}, key: {}, size:{}, cost time: {}",
                        record.topic(), record.offset(), record.key(), sb.length(), System.currentTimeMillis() - start);
            } catch (Exception e) {
                statManger.remove(record.key(), totalCnt);
                throw e;
            }
        }
    }

    private void sendStatAndHeartBeat(DBusConsumerRecord<String, byte[]> data) {
        String[] dataKeys = StringUtils.split(data.key(), ".");
        String alias = inner.dsNameAlias.get(String.format("%s.%s", dataKeys[2], dataKeys[3]));
        if (alias == null) {
            alias = dataKeys[2];
        }
        StatMessage sm = new StatMessage(alias, dataKeys[3], dataKeys[4], SinkerConstants.SINKER_TYPE);
        Long curTime = System.currentTimeMillis();
        String times[] = StringUtils.split(dataKeys[8], "|");
        Long time = Long.valueOf(times[0]);
        sm.setCheckpointMS(time);
        sm.setTxTimeMS(time);
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - time);

        String statKey = getStatKey(dataKeys);
        final Stat stat = statManger.get(statKey);
        if (stat != null) {
            sm.setCount(stat.getSuccessCnt());
            sm.setErrorCount(stat.getErrorCnt());
            logger.debug("[stat] send stat message {}", sm.toJSONString());
        } else {
            sm.setCount(0);
            sm.setErrorCount(0);
            logger.debug("[stat] send stat message {}", sm.toJSONString());
        }

        String statTopic = inner.sinkerConfProps.getProperty(SinkerConstants.STAT_TOPIC);
        producer.send(new ProducerRecord<>(statTopic, String.format("%s.%s.%s", alias, dataKeys[3], dataKeys[4]), sm.toJSONString().getBytes()), (metadata, exception) -> {
            if (exception != null) {
                logger.error("[stat] send stat message fail.", exception);
            }
        });
        String heartbeatTopic = inner.sinkerConfProps.getProperty(SinkerConstants.SINKER_HEARTBEAT_TOPIC);
        String heartbeatKey = String.format("%s|%s|%s|%s", String.format("%s.%s.%s", alias, dataKeys[3], dataKeys[4]), time, curTime, curTime - time);
        producer.send(new ProducerRecord<>(heartbeatTopic, heartbeatKey, null), (metadata, exception) -> {
            if (exception != null) {
                logger.error("[heartbeat] send heartbeat message fail.", exception);
            }
        });
        if (stat != null) stat.success();
    }

    private void processCtrlMsg(JSONObject json) throws Exception {
        logger.info("[write bolt] process ctrl msg. cmd:{}", json);
        ControlType cmd = ControlType.getCommand(json.getString("type"));
        switch (cmd) {
            case SINKER_RELOAD_CONFIG:
                reload(json.toJSONString());
                break;
            default:
                break;
        }
    }

    private SinkerWriteHandler getSinkerWriteHandler() {
        switch (inner.sinkType) {
            case "hdfs":
                return new SinkerHdfsWritehandler();
            default:
                throw new RuntimeException("not support sink type .");
        }
    }

    private int countDataRows(String str) {
        int count = 0;
        int index = 0;
        String findStr = "{\"tuple\":[";
        while ((index = str.indexOf(findStr, index)) != -1) {
            index += findStr.length();
            count++;
        }
        return count;
    }

    private void init() throws Exception {
        inner.init();
        this.inner.initSinker();
        initProducer();
        this.statManger = new MessageStatManger();
    }

    private void destroy() {
        logger.info("[write bolt] reload close producer");
        if (producer != null) producer.close();
        inner.close();
    }

    private void reload(String strCtrl) throws Exception {
        destroy();
        init();
        logger.info("[write bolt] load completed.");
        inner.zkHelper.saveReloadStatus(strCtrl, "SinkerKafkaReadSpout-" + context.getThisTaskId(), true);
        logger.info("[write bolt] reload completed.");
    }

    private void initProducer() throws Exception {
        Properties properties = inner.zkHelper.loadSinkerConf(SinkerConstants.PRODUCER);
        properties.put("client.id", inner.topologyId + "SinkerWriteClient" + context.getThisTaskId());
        this.producer = new KafkaProducer<>(properties);
        logger.info("[write bolt] create kafka producer.");
        logger.info("[write bolt] init monitor producer success with task index: {}", context.getThisTaskId());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("data"));
        declarer.declareStream("ctrlStream", new Fields("data"));
    }

    private boolean isCtrl(List<DBusConsumerRecord<String, byte[]>> data) {
        return data.size() == 1 && StringUtils.equals(data.get(0).key(), ControlType.SINKER_RELOAD_CONFIG.name());
    }

    public boolean isHeartBeat(DBusConsumerRecord<String, byte[]> data) {
        return data.key().startsWith("data_increment_heartbeat");
    }

    private boolean isUMS(DBusConsumerRecord<String, byte[]> data) {
        return data.key().startsWith("data_increment_data");
    }

    private String getStatKey(String[] dataKeys) {
        return String.format("%s.%s.%s", dataKeys[2], dataKeys[3], dataKeys[4]);
    }

}
