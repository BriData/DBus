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


package com.creditease.dbus.stream.dispatcher.bout;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.stream.common.bean.DispatcherPackage;
import com.creditease.dbus.stream.dispatcher.helper.ZKHelper;
import com.creditease.dbus.stream.dispatcher.tools.FullyOffset;
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

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaProducerBout extends BaseRichBolt {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private final static MetricRegistry metrics = new MetricRegistry();

    //用于内部统计
    private Slf4jReporter reporter = null;
    private Meter messagesMeter = null;
    private Meter recordsMeter = null;
    private Meter bytesMeter = null;


    private String zkServers = null;
    private String topologyID = null;
    private String topologyRoot = null;

    private Map conf = null;
    private TopologyContext context = null;
    private OutputCollector collector = null;

    private KafkaProducer<String, byte[]> producer = null;

    private void startReport() {
        reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger("com.creditease.dbus.metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(10, TimeUnit.SECONDS);

        messagesMeter = metrics.meter("D-Kafka_Messages");
        recordsMeter = metrics.meter("D-Records");
        bytesMeter = metrics.meter("D-Bytes");
    }

    private void reloadConfig() {
        cleanup();

        ZKHelper zkHelper = null;

        try {
            //producer 的初始化
            zkHelper = new ZKHelper(topologyRoot, topologyID, zkServers);
            producer = new KafkaProducer<>(zkHelper.getProducerProps());
            logger.info("producer config loaded.");

            //metrics
            startReport();

        } catch (Exception ex) {
            logger.error("KafkaProducerBout reloadConfig():", ex);
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

        //only init one time
        zkServers = (String) stormConf.get(Constants.ZOOKEEPER_SERVERS);
        topologyID = (String) stormConf.get(Constants.TOPOLOGY_ID);
        topologyRoot = Constants.TOPOLOGY_ROOT + "/" + topologyID;

        reloadConfig();

        logger.info("KafkaProducerBolt reload config success !");
    }

    @Override
    public void execute(Tuple input) {

        DispatcherPackage subPackage = (DispatcherPackage) input.getValueByField("subPackage");
        String toTopic = subPackage.getToTopic();
        String key = subPackage.getKey();
        byte[] message = subPackage.getContent();
        int msgCount = subPackage.getMsgCount();
        FullyOffset currentOffset = (FullyOffset) input.getValueByField("currentOffset");


        //total++
        messagesMeter.mark();
        recordsMeter.mark(msgCount);
        bytesMeter.mark(message.length);

        try {

            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata ignored, Exception e) {
                    synchronized (collector) {
                        if (e != null) {
                            collector.fail(input);
                        } else {
                            collector.ack(input);
                        }
                    }

                    if (e != null) {
                        logger.error(e.getMessage(), new RuntimeException(e));
                        logger.error(String.format("Send FAIL: toTopic=%s, currentOffset=%s", toTopic, currentOffset.toString()));
                    } else {
                        logger.debug(String.format("  Send successful: currentOffset=%s, toTopic=%s, key=(%s)", currentOffset.toString(), toTopic, key));

                    }
                }
            };

            logger.info(String.format("  Sending: currentOffset=%s, toTopic=%s, key=(%s), msgCount=%d", currentOffset.toString(), toTopic, key, msgCount));
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(toTopic, key, message), callback);

        } catch (Exception ex) {
            // Print something in the log
            logger.error(String.format("FAIL! Kafka bolt fails at offset (%s).", currentOffset.toString()));
            // Call fail
            this.collector.fail(input);

            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void cleanup() {
        if (producer != null) {
            producer.close();
        }

        if (reporter != null) {
            reporter.stop();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
