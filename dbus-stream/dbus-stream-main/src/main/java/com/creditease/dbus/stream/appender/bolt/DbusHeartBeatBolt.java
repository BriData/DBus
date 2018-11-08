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

package com.creditease.dbus.stream.appender.bolt;

import com.creditease.dbus.commons.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.common.appender.kafka.DataOutputTopicProvider;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltHandlerManager;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.HeartbeatHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 负责生成心跳的
 * Created by Shrimp on 16/6/2.
 */
public class DbusHeartBeatBolt extends BaseRichBolt implements HeartbeatHandlerListener {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private OutputCollector collector;
    private Producer<String, String> producer;
    private boolean initialized = false;
    private TopicProvider topicProvider;

    private String topologyId;
    private String datasource;
    private String zkconnect;
    private String zkRoot;

    private BoltHandlerManager handlerManager;
    private TopologyContext context;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!initialized) {
            this.topologyId = (String) conf.get(Constants.StormConfigKey.TOPOLOGY_ID);
            this.datasource = (String) conf.get(Constants.StormConfigKey.DATASOURCE);
            this.zkconnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
            this.zkRoot = Utils.buildZKTopologyPath(topologyId);
            try {
                PropertiesHolder.initialize(zkconnect, zkRoot);
                producer = createProducer(context.getThisTaskId());
                topicProvider = new DataOutputTopicProvider();
                GlobalCache.initialize(datasource);
                handlerManager = new BoltHandlerManager(buildProvider());
                initialized = true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new InitializationException(e);
            }
        }
    }

    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            collector.ack(input);
            return;
        }
        try {
            Command cmd = (Command) input.getValueByField(Constants.EmitFields.COMMAND);
            BoltCommandHandler handler = handlerManager.getHandler(cmd);
            handler.handle(input);
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.ack(input);
            logger.error("Heartbeat error, ignore this error", e);
        }
    }

    @Override
    public void sendHeartbeat(DbusMessage message, String topic, String key) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message.toString());
        producer.send(record);
        //  logger.info("Write heartbeat message to kafka:{topic:{}, key:{}}", record.topic(), record.key());
        //logger.info("Write heartbeat message to kafka:{topic:{}, key:{}, message:{}}", record.topic(), record.key(), record.value());
    }

    @Override
    public String getTargetTopic(String dbSchema, String tableName) {
        List<String> topics = topicProvider.provideTopics(dbSchema, tableName);
        if (topics != null && !topics.isEmpty()) {
            return topics.get(0);
        }
        return null;
    }

    @Override
    public void reloadBolt(Tuple tuple) {
        String msg = null;
        try {
            logger.info("Begin to reload local cache!");
            PropertiesHolder.reload();
            GlobalCache.initialize(datasource);
            ThreadLocalCache.reload();
            Command.initialize();
            if (producer != null) {
                producer.close();
            }
            this.producer = createProducer(context.getThisTaskId());
            msg = "heartbeat bolt reload successful!";
            logger.info("Heartbeat bolt was reloaded at:{}", System.currentTimeMillis());
        } catch (Exception e) {
            msg = e.getMessage();
            logger.error("Reload heartbeat bolt error", e);
        } finally {
            if (tuple != null) {
                EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
                ControlMessage message = data.get(EmitData.MESSAGE);
                CtlMessageResult result = new CtlMessageResult("heartbeat-bolt", msg);
                result.setOriginalMessage(message);
                CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkconnect);
                sender.send("heartbeat-bolt", result, false, true);
            }
        }
    }

    @Override
    public OutputCollector getOutputCollector() {
        return this.collector;
    }

    private Producer<String, String> createProducer(int taskId) throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
        Properties properties = new Properties();
        properties.putAll(props);
        properties.setProperty("client.id", this.topologyId + "_heartbeat_" + taskId);

        Producer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private BoltCommandHandlerProvider buildProvider() throws Exception {
        /*Set<Class<?>> classes = AnnotationScanner.scan("com.creditease.dbus", BoltCmdHandlerProvider.class, (clazz, annotation) -> {
            BoltCmdHandlerProvider ca = clazz.getAnnotation(BoltCmdHandlerProvider.class);
            return ca.type() == BoltType.HEARTBEAT_BOLT;
        });
        if (classes.size() <= 0) throw new ProviderNotFoundException();
        if (classes.size() > 1) {
            String providers = "";
            for (Class<?> clazz : classes) {
                providers += clazz.getName() + " ";
            }
            throw new RuntimeException("too many providers found: " + providers);
        }*/
        DbusDatasourceType type = GlobalCache.getDatasourceType();
        String name;
        if(type == DbusDatasourceType.ORACLE) {
            name = "com.creditease.dbus.stream.oracle.appender.bolt.processor.provider.HeartbeatCmdHandlerProvider";
        } else if(type == DbusDatasourceType.MYSQL) {
            name = "com.creditease.dbus.stream.mysql.appender.bolt.processor.provider.HeartbeatCmdHandlerProvider";
        }
        else {
            throw new IllegalArgumentException("Illegal argument [" + type.toString() + "] for building BoltCommandHandler map!");
        }
        Class<?> clazz = Class.forName(name);
        Constructor<?> constructor = clazz.getConstructor(HeartbeatHandlerListener.class);
        return (BoltCommandHandlerProvider) constructor.newInstance(this);
    }
}
