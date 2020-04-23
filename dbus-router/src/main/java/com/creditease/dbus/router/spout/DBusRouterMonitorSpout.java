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


package com.creditease.dbus.router.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.creditease.dbus.router.bolt.manager.KafkaConsumerManager;
import com.creditease.dbus.router.spout.aware.KafkaConsumerAware;
import com.creditease.dbus.router.spout.context.ProcessorContext;
import com.creditease.dbus.router.spout.handler.ConsumerRecordsHandler;
import com.creditease.dbus.router.spout.handler.Handler;
import com.creditease.dbus.router.spout.handler.processor.AbstractProcessor;
import com.creditease.dbus.router.spout.handler.processor.MonitorSpoutControlProcessor;
import com.creditease.dbus.router.spout.handler.processor.MonitorSpoutDataProcessor;
import com.creditease.dbus.router.spout.handler.processor.MonitorSpoutProcessorChain;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mal on 2018/7/09.
 */
public class DBusRouterMonitorSpout extends BaseSpout {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterMonitorSpout.class);

    private static List<AbstractProcessor> processors = new ArrayList<>();

    static {
        processors.add(new MonitorSpoutControlProcessor());
        processors.add(new MonitorSpoutDataProcessor());
    }

    private ProcessorContext processorContext = null;
    private KafkaConsumerManager consumerManager = null;
    private Handler handler = null;

    @Override
    public void postOpen() throws Exception {
        consumerManager = new KafkaConsumerManager(inner.zkHelper.loadKafkaConsumerConf(), inner.zkHelper.getSecurityConf(), "monitor");
        processorContext = new ProcessorContext(this, this.inner, inner.dbHelper.loadSinks(inner.topologyId));
        handler = new ConsumerRecordsHandler(new MonitorSpoutProcessorChain(processorContext, processors));
        initConsumer();
        logger.info("monitor spout open completed");
    }

    private void propagationKafkaConsumerAware(KafkaConsumer consumer) {
        if (KafkaConsumerAware.class.isAssignableFrom(handler.getClass()))
            ((KafkaConsumerAware) handler).setKafkaConsumer(consumer);
    }

    @Override
    public void nextTuple() {
        consumerManager.foreach((url, consumer) -> {
            KafkaConsumer<String, byte[]> kafkaConsumer = (KafkaConsumer) consumer;
            propagationKafkaConsumerAware(kafkaConsumer);
            return (Boolean) handler.handle(kafkaConsumer.poll(0));
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void close() {
        consumerManager.close();
        inner.close(false);
    }

    private String obtainCtrlTopic() {
        return StringUtils.joinWith("_", inner.topologyId, "ctrl");
    }

    @Override
    public void initConsumer() throws Exception {
        consumerManager.close();
        consumerManager.subscribe(inner.zkHelper.loadKafkaConsumerConf().getProperty("bootstrap.servers"), obtainCtrlTopic(), true);
        Optional.ofNullable(processorContext.getSinks()).ifPresent(sinks -> sinks.forEach(sink -> consumerManager.subscribe(sink.getUrl(), sink.getTopic(), false)));
    }


}
