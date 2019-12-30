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


package com.creditease.dbus.log.processor.bolt;


import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.log.processor.base.LogProcessorBase;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.util.DateUtil;
import com.creditease.dbus.log.processor.window.HeartBeatWindowInfo;
import com.creditease.dbus.log.processor.window.StatWindowInfo;
import org.apache.commons.lang3.StringUtils;
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

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;

public class LogProcessorKafkaWriteBolt extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(LogProcessorKafkaWriteBolt.class);

    private TopologyContext context = null;
    private OutputCollector collector = null;
    private LogProcessorKafkaWriteBoltInner inner = null;
    private Properties kafkaProducerConf = null;
    private KafkaProducer<String, byte[]> producer = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        inner = new LogProcessorKafkaWriteBoltInner(conf);
        init();
        logger.info("LogProcessorKafkaWriteBolt is started!");
    }

    private void init() {
        inner.loadConf();
        initProducer();
    }

    private void initProducer() {
        kafkaProducerConf = inner.zkHelper.loadKafkaProducerConf();
        producer = new KafkaProducer<>(kafkaProducerConf);
    }

    @Override
    public void execute(Tuple input) {
        try {
            String emitDataType = (String) input.getValueByField("emitDataType");
            switch (emitDataType) {
                case Constants.EMIT_DATA_TYPE_CTRL: {
                    String value = (String) input.getValueByField("value");
                    processControlCommand(value, input);
                    break;
                }
                case Constants.EMIT_DATA_TYPE_HEARTBEAT: {
                    String outputTopic = (String) input.getValueByField("outputTopic");
                    HeartBeatWindowInfo hbwi = (HeartBeatWindowInfo) input.getValueByField("value");
                    String strTs = DateUtil.convertLongToStr4Date(hbwi.getTimestamp());
                    logger.info("heartbeat info [ time: {}, table：{} , status: {}]", strTs, StringUtils.joinWith("|", hbwi.getHost(), hbwi.getNamespace()), hbwi.getStatus());
                    writeDataToTopic(outputTopic, buildKey(hbwi.getDbusMessage(), hbwi.getTimestamp(), hbwi.getStatus()), hbwi.getDbusMessage().toString(), input);
                    break;
                }
                case Constants.EMIT_DATA_TYPE_NORMAL: {
                    String outputTopic = (String) input.getValueByField("outputTopic");
                    DbusMessage ums = (DbusMessage) input.getValueByField("value");
                    writeDataToTopic(outputTopic, buildKey(ums), ums.toString(), input);
                    break;
                }
                case Constants.EMIT_DATA_TYPE_STAT: {
                    String outputTopic = (String) input.getValueByField("outputTopic");
                    StatWindowInfo swi = (StatWindowInfo) input.getValueByField("value");
                    String strTs = DateUtil.convertLongToStr4Date(swi.getTimestamp());
                    logger.info("stat info [ time: {}, table：{}, success:{}, readKafkaCount: {}]",
                            strTs, StringUtils.joinWith("|", swi.getHost(), swi.getNamespace()), swi.getSuccessCnt(), swi.getReadKafkaCount());
                    writeDataToTopic(outputTopic, "", getStatMessage(swi).toJSONString(), input);
                    break;
                }
                default:
                    break;
            }
        } catch (Exception e) {
            logger.error("LogProcessorKafkaWriteBolt execute error:", e);
            collector.fail(input);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private String buildKey(DbusMessage dbusMessage) {
        // 普通 data_increment_data.log-logstash.dsName.schemaName.tableName.5.0.0.time.wh_placeholder
        // 普通 data_increment_data.log-ums.dsName.schemaName.tableName.5.0.0.time.wh_placeholder
        // 例子 data_increment_data.log-logstash.db1.schema1.table1.5.0.0.1481245701166.wh_placeholder
        long opts;
        try {
            opts = DateUtil.getTimeMills(dbusMessage.messageValue(DbusMessage.Field._UMS_TS_, 0).toString());
        } catch (ParseException e) {
            opts = System.currentTimeMillis();
        }
        String type = dbusMessage.getProtocol().getType();
        String ns = dbusMessage.getSchema().getNamespace();

        //TODO joinWith
        return StringUtils.join(type, ".", ns, "." + opts + ".", "wh_placeholder");
    }

    private String buildKey(DbusMessage dbusMessage, Long timeStamp, String status) {
        // 例子 data_increment_heartbeat.log-logstash.db1.schema1.table1.5.0.0.1481245701166|1481245700947|wh_placeholder
        // data_increment_heartbeat.mysql.db1.schema1.table1.5.0.0.1481245701166|1481245700947|ok.wh_placeholder
        String type = dbusMessage.getProtocol().getType();
        String ns = dbusMessage.getSchema().getNamespace();
        return StringUtils.join(type, ".", inner.logProcessorConf.getProperty("log.type"), ".", ns, ".", timeStamp, "|", timeStamp, "|", status, ".wh_placeholder");
    }

    private StatMessage getStatMessage(StatWindowInfo swi) {
        String[] arr = StringUtils.split(swi.getNamespace(), '|');
        //dsName, schemaName, tableName, type
        StatMessage sm = new StatMessage(arr[0], arr[1], arr[2], inner.logProcessorConf.getProperty("log.type") + "-" + swi.getHost());
        Long curTime = System.currentTimeMillis();
        sm.setCheckpointMS(swi.getTimestamp());
        sm.setTxTimeMS(swi.getTimestamp());
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - swi.getTimestamp());
        sm.setCount(swi.getSuccessCnt());
        sm.setErrorCount(swi.getErrorCnt());
        return sm;
    }

    @Override
    public void cleanup() {
        inner.close(false);
        if (producer != null) producer.close();
    }

    private class LogProcessorKafkaWriteBoltInner extends LogProcessorBase {
        public LogProcessorKafkaWriteBoltInner(Map conf) {
            super(conf);
        }
    }

    private void processControlCommand(String value, Tuple input) {
        try {
            ControlType cmd = ControlType.getCommand(JSONObject.parseObject(value).getString("type"));
            switch (cmd) {
                case LOG_PROCESSOR_RELOAD_CONFIG: {
                    logger.info("LogProcessorKafkaWriteBolt-{} 收到reload消息！Type: {}, Values: {} ", context.getThisTaskId(), cmd, value);
                    inner.close(true);
                    if (producer != null) producer.close();
                    init();
                    inner.zkHelper.saveReloadStatus(value, "LogProcessorKafkaWriteBolt-" + context.getThisTaskId(), true);
                    break;
                }
                default:
                    break;
            }
            collector.ack(input);

        } catch (Exception e) {
            logger.error("LogProcessorTransformBolt processControlCommand():", e);
            collector.reportError(e);
            collector.fail(input);
        }
    }


    private void writeDataToTopic(String outputTopic, String key, String data, Tuple input) throws UnsupportedEncodingException {
        // 为了使用现有的心跳报警机制，key的形式采用和apprender相同的格式，具体如下所示：
        //新的格式有 10个字段
        //      普通 data_increment_data.log-logstash.dsName.schemaName.tableName.5.0.0.time.wh
        //      例子 data_increment_data.log-logstash.db1.schema1.table1.5.0.0.1481245701166.wh
        //      心跳 data_increment_heartbeat.log-logstash.dsName.schemaName.tableName.time|txTime|ok.wh
        //      例子 data_increment_heartbeat.log-logstash.db1.schema1.table1.5.0.0.1481245701166|1481245700947|ok.wh
        producer.send(new ProducerRecord<>(outputTopic, key, data.getBytes("UTF-8")), new Callback() {
            @Override
            public void onCompletion(RecordMetadata re, Exception e) {
                synchronized (collector) {
                    if (e != null) {
                        collector.fail(input);
                    } else {
                        collector.ack(input);
                    }
                }
            }
        });
    }

}
