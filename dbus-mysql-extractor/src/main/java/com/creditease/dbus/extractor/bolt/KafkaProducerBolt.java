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

package com.creditease.dbus.extractor.bolt;

import com.creditease.dbus.extractor.common.utils.Constants;
import com.creditease.dbus.extractor.common.utils.ZKHelper;
import com.creditease.dbus.extractor.container.ExtractorConfigContainer;
import com.creditease.dbus.extractor.container.KafkaContainer;
import com.creditease.dbus.extractor.vo.MessageVo;
import com.creditease.dbus.extractor.vo.OutputTopicVo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by ximeiwang on 2017/8/15.
 */
public class KafkaProducerBolt extends BaseRichBolt {
    //TODO
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private Map conf = null;
    private OutputCollector collector = null;
    private TopologyContext context = null;

    private Producer producer;
    private String outputTopic = null;

    private String zkServers;

    private String extractorName;
    private String extractorRoot;

    private void reloadConfig(String reloadJson) {
        //TODO
        logger.info("kafka producer bolt reload configure starting......");
        ZKHelper zkHelper = null;
        try {
            zkHelper = new ZKHelper(zkServers, extractorRoot, extractorName);
            zkHelper.loadJdbcConfig();
            zkHelper.loadExtractorConifg();
            zkHelper.loadOutputTopic();
            zkHelper.loadKafkaProducerConfig();

            if (producer != null) {
                producer.close();
                producer = null;
            }
            producer = KafkaContainer.getInstance()
                    .getProducer(ExtractorConfigContainer.getInstances().getKafkaProducerConfig());
            for (OutputTopicVo vo : ExtractorConfigContainer.getInstances().getOutputTopic()) {
                outputTopic = vo.getTopic();
                break;
            }

            //写reload状态到zookeeper
            zkHelper.saveReloadStatus(reloadJson, "extractor-kafka-producer-bolt", false);

            logger.info("kafka producer bolt reload configure succeed!");
        } catch (Exception ex) {
            collector.reportError(ex);
            throw new RuntimeException(ex);
        } finally {
            if (zkHelper != null) {
                zkHelper.close();
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.collector = collector;
        this.context = context;

        this.zkServers = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        this.extractorName = (String) conf.get(Constants.EXTRACTOR_TOPOLOGY_ID);
        this.extractorRoot = Constants.EXTRACTOR_ROOT + "/";
        reloadConfig(null);

    }

    @Override
    public void execute(Tuple input) {
        try {
            if (input.getValueByField("reloadControl") instanceof ConsumerRecord) {
                ConsumerRecord<String, byte[]> reloadRecord = (ConsumerRecord<String, byte[]>) input.getValueByField("reloadControl");
                if (reloadRecord != null) {
                    String key = reloadRecord.key();
                    if (key.equals("EXTRACTOR_RELOAD_CONF")) {
                        String json = new String(reloadRecord.value(), "utf-8");
                        logger.info("kafka producer bolt receive reload configure control. the event is {}.", json);
                        reloadConfig(json);
                        return;
                    }
                }
            }
            MessageVo msgVo = (MessageVo) input.getValueByField("message");
            if (msgVo != null) {
                logger.debug("execute kafka send the message which batchId is {} ", msgVo.getBatchId());
                sendDataToKafka(msgVo.getBatchId(), msgVo.getMessage(), input);
                //logger.info("execute kafka send the message which batchId is {} ", msgVo.getBatchId());
            }
        } catch (Exception e) {
            logger.info("execute error");
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    private void sendDataToKafka(long batchId, byte[] data, Tuple input) {
        @SuppressWarnings("rawtypes")
        ProducerRecord record = new ProducerRecord<>(outputTopic, "", data);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                synchronized (collector) {
                    if (e != null) {
                        collector.fail(input);
                        logger.error("kafka ack failed to the message which batchId is " + batchId, e);
                    } else {
                        collector.ack(input);
                        logger.info("kafka ack to the message which batchId is " + batchId, e);
                    }
                }
            }
        });
    }
}

