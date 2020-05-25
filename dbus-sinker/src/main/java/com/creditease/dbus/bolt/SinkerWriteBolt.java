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
import com.creditease.dbus.bean.HdfsOutputStreamInfo;
import com.creditease.dbus.bolt.stat.MessageStatManger;
import com.creditease.dbus.bolt.stat.Stat;
import com.creditease.dbus.cache.LocalCache;
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
import java.util.Set;

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
        String statKey = getStatKey(record.key());
        int totalCnt = 0;
        StringBuilder sb = new StringBuilder();

        // 数据demo
        DBusConsumerRecord<String, byte[]> dataRecord = null;
        String dataVersion = null;
        for (int i = 0; i < data.size(); i++) {
            DBusConsumerRecord<String, byte[]> consumerRecord = data.get(i);
            // heartbeat消息不进hdfs
            if (isHeartBeat(consumerRecord.key())) {
                sendStatAndHeartBeat(statKey, consumerRecord.key());
            } else if (isData(consumerRecord.key())) {
                if (dataVersion == null) {
                    dataVersion = getVersion(consumerRecord.key());
                    dataRecord = consumerRecord;
                } else if (!dataVersion.equals(getVersion(consumerRecord.key()))) {
                    // 由于send方法不处理数据,这里需要对表结构变更进行处理,发生表结构变更直接发送数据
                    send(start, statKey, totalCnt, sb, dataRecord);
                    // 数据计数清零
                    sb.delete(0, sb.length());
                    totalCnt = 0;

                    dataRecord = consumerRecord;
                }
                // 为数据添加换行符
                String str = new String(consumerRecord.value(), "utf-8") + "\n";
                int cnt = countDataRows(str);
                statManger.put(statKey, consumerRecord.key(), cnt);
                totalCnt += cnt;
                sb.append(str);
                statKey = getStatKey(consumerRecord.key());
                logger.debug("[count] {},{},{},{},{}", start, i, statKey, cnt, consumerRecord.key());
            } else {
                logger.warn("[write bolt] not support process type. emit warp key: {}", data);
            }
        }

        // 发送剩余数据
        send(start, statKey, totalCnt, sb, dataRecord);
    }

    private void send(long start, String statKey, int totalCnt, StringBuilder sb, DBusConsumerRecord<String, byte[]> dataRecord) throws Exception {
        logger.debug("[count] {},total {},size {}", start, totalCnt, sb.length());
        if (totalCnt > 0) {
            try {
                SinkerWriteHandler sinkerWriteHandler = getSinkerWriteHandler();
                sinkerWriteHandler.sendData(inner, dataRecord, sb.toString());
                logger.debug("[write hdfs] topic: {}, offset: {}, key: {}, size:{}, cost time: {}",
                        dataRecord.topic(), dataRecord.offset(), dataRecord.key(), sb.length(), System.currentTimeMillis() - start);
            } catch (Exception e) {
                statManger.remove(statKey, totalCnt);
                throw e;
            }
        }
    }

    private String getVersion(String key) {
        return StringUtils.split(key, ".")[5];
    }

    private void sendStatAndHeartBeat(String statKey, String key) {
        String[] dataKeys = StringUtils.split(key, ".");
        String dsName = dataKeys[2];
        String[] split8 = StringUtils.split(dataKeys[8], "|");
        if (split8.length > 3) {
            dsName = split8[3];
        }
        StatMessage sm = new StatMessage(dsName, dataKeys[3], dataKeys[4], SinkerConstants.SINKER_TYPE);

        Long curTime = System.currentTimeMillis();
        Long time = Long.valueOf(split8[0]);
        sm.setCheckpointMS(time);
        sm.setTxTimeMS(Long.valueOf(split8[1]));
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - time);

        final Stat stat = statManger.get(statKey);
        if (stat != null) {
            sm.setCount(stat.getSuccessCnt());
            sm.setErrorCount(stat.getErrorCnt());
            logger.debug("[stat] send stat message {}", sm.toJSONString());
            stat.success();
        } else {
            sm.setCount(0);
            sm.setErrorCount(0);
            logger.debug("[stat] send stat message [0] ,{}", sm.toJSONString());
        }

        String statTopic = inner.sinkerConfProps.getProperty(SinkerConstants.STAT_TOPIC);
        producer.send(new ProducerRecord<>(statTopic, statKey, sm.toJSONString().getBytes()), (metadata, exception) -> {
            if (exception != null) {
                logger.error("[stat] send stat message fail.", exception);
            }
        });
        String heartbeatTopic = inner.sinkerConfProps.getProperty(SinkerConstants.SINKER_HEARTBEAT_TOPIC);
        String heartbeatKey = String.format("%s|%s|%s|%s", statKey, time, curTime, curTime - time);
        producer.send(new ProducerRecord<>(heartbeatTopic, heartbeatKey, null), (metadata, exception) -> {
            if (exception != null) {
                logger.error("[heartbeat] send heartbeat message fail.", exception);
            }
        });
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
        properties.put("client.id", inner.sinkerName + "SinkerWriteClient" + context.getThisTaskId());
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

    public boolean isHeartBeat(String key) {
        return key.startsWith("data_increment_heartbeat");
    }

    private boolean isData(String key) {
        return key.startsWith("data_increment_data");
    }

    private String getStatKey(String key) {
        if (isHeartBeat(key)) {
            return getHeartBeatStatKey(key);
        }
        if (isData(key)) {
            return getDataStatKey(key);
        }
        return null;
    }

    private String getDataStatKey(String key) {
        //之前的dataKey
        //data_increment_data.oracle.acc3.ACC.REQUEST_BUS_SHARDING_2018_0000.0.0.0.1577810117698.wh_placeholder
        //20200114之后的dataKey,为了兼容数据线的别名
        //data_increment_data.oracle.acc3.ACC.REQUEST_BUS_SHARDING_2018_0000.0.0.0.1577810117698|acc3.wh_placeholder
        String[] dataKeys = StringUtils.split(key, ".");
        String dsName = dataKeys[2];
        String[] split8 = StringUtils.split(dataKeys[8], "|");
        if (split8.length > 1) {
            dsName = split8[1];
        }
        return String.format("%s.%s.%s", dsName, dataKeys[3], dataKeys[4]);
    }

    private String getHeartBeatStatKey(String key) {
        //之前的heartbeatKey
        //data_increment_heartbeat.mysql.mysql_db5.ip_settle.fin_credit_repayment_bill.0.0.0.1578894885250|1578894877783|ok.wh_placeholder
        //20200114之后的heartbeatKey,为了兼容数据线的别名
        //data_increment_heartbeat.mysql.mysql_db5.ip_settle.fin_credit_repayment_bill.0.0.0.1578894885250|1578894877783|ok|mysql_db24.wh_placeholder
        String[] dataKeys = StringUtils.split(key, ".");
        String dsName = dataKeys[2];
        String[] split8 = StringUtils.split(dataKeys[8], "|");
        if (split8.length > 3) {
            dsName = split8[3];
        }
        return String.format("%s.%s.%s", dsName, dataKeys[3], dataKeys[4]);
    }

}
