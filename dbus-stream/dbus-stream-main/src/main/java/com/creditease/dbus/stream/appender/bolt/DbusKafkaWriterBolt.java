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


package com.creditease.dbus.stream.appender.bolt;

import com.creditease.dbus.commons.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.appender.utils.AppenderMetricReporter;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.Constants.EmitFields;
import com.creditease.dbus.stream.common.Constants.StormConfigKey;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltHandlerManager;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.KafkaBoltHandlerListener;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.StatSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.TableMessageStatReporter;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.kafka.DataOutputTopicProvider;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Joiner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Shrimp on 16/6/2.
 */
public class DbusKafkaWriterBolt extends BaseRichBolt implements KafkaBoltHandlerListener {

    private Logger logger = LoggerFactory.getLogger(getClass());
    //private static final int SENT_QUEUE_SIZE = 1500000;

    private String zkRoot;
    private String topologyId;
    private String datasource;
    private boolean initialized = false;

    private OutputCollector collector;
    private TopicProvider topicProvider;
    private BoltHandlerManager handlerManager;
    private Producer<String, String> producer;
    private AppenderMetricReporter reporter;
    //private IndexedEvictingQueue evictingQueue;
    private TopologyContext context;
    private TableMessageStatReporter tableStatReporter;
    private StatSender statSender;
    private String zkconnect;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!initialized) {
            this.topologyId = (String) conf.get(StormConfigKey.TOPOLOGY_ID);
            this.datasource = (String) conf.get(StormConfigKey.DATASOURCE);
            this.zkRoot = Utils.buildZKTopologyPath(topologyId);
            try {
                this.zkconnect = (String) conf.get(StormConfigKey.ZKCONNECT);
                PropertiesHolder.initialize(this.zkconnect, zkRoot);
                GlobalCache.initialize(datasource);

//                String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.DBUS_STATISTIC_TOPIC);

                if (producer != null) {
                    producer.close();
                }
                producer = createProducer();

                topicProvider = new DataOutputTopicProvider();
                reporter = AppenderMetricReporter.getInstance();
                //evictingQueue = IndexedEvictingQueue.create(SENT_QUEUE_SIZE);

                statSender = new KafkaStatSender();
                tableStatReporter = new TableMessageStatReporter(statSender);

                handlerManager = new BoltHandlerManager(buildProvider());

                initialized = true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new InitializationException(e);
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            collector.ack(input);
            return;
        }

        try {
            Command cmd = (Command) input.getValueByField(EmitFields.COMMAND);
            BoltCommandHandler handler = handlerManager.getHandler(cmd);
            handler.handle(input);
        } catch (Exception e) {
            logger.error("Process data error", e);
            this.collector.reportError(e);
            this.collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EmitFields.GROUP_FIELD, EmitFields.DATA, EmitFields.COMMAND, EmitFields.EMIT_TYPE));
    }

    @Override
    public OutputCollector getOutputCollector() {
        return collector;
    }

    @Override
    public void reloadBolt(Tuple tuple) {
        String msg = null;
        try {
            PropertiesHolder.reload();
            ThreadLocalCache.reload();
            if (producer != null) {
                producer.close();
            }

            producer = createProducer();
            msg = "kafka write bolt reload successful!";
            logger.info("Kafka writer bolt was reloaded at:{}", System.currentTimeMillis());
        } catch (Exception e) {
            msg = e.getMessage();
            throw new RuntimeException(e);
        } finally {
            if (tuple != null) {
                EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
                ControlMessage message = data.get(EmitData.MESSAGE);
                CtlMessageResult result = new CtlMessageResult("kafka-write-bolt", msg);
                result.setOriginalMessage(message);
                CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkconnect);
                sender.send("kafka-write-bolt-" + context.getThisTaskId(), result, false, true);
            }
        }
    }

    @Override
    public void writeData(String dbSchema, String table, DbusMessage dbusMessage, Tuple input) {
        EmitData data = (EmitData) input.getValueByField(Constants.EmitFields.DATA);
        List<String> topics = topicProvider.provideTopics(dbSchema, table);
        if (topics == null || topics.isEmpty()) {
            logger.error("Can't find a topic to write the message!");
            this.collector.ack(input);
            return;
        }

        String message = dbusMessage.toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topics.get(0), BoltCommandHandlerHelper.buildKey(dbusMessage), message);
        reporter.report(message.getBytes().length, dbusMessage.getPayload().size());
        Object offsetObj = data.get(EmitData.OFFSET);
        String offset = offsetObj != null ? offsetObj.toString() : "0";
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                synchronized (this.collector) {
                    this.collector.fail(input);
                }
                logger.error("Write data to kafka error, original data:[schema:{}, table:{}, offset:{}]!", dbSchema, table, offset, exception);
            } else {
                synchronized (this.collector) {
                    this.collector.ack(input);
                }
                logger.debug("[appender-kafka-writer] kafka-message was sent, original-offset:{}, key:{}", offset, record.key());
            }
        });
    }

    @Override
    public void sendHeartbeat(DbusMessage message, String topic, String key) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message.toString());
        producer.send(record, (metadata, e) -> {
            if (e != null) {
                logger.warn("write heartbeat message error.", e);
            }
        });
        logger.debug("Write heartbeat message to kafka:{topic:{}, key:{}, message:{}}", record.topic(), record.key(), record.value());
    }

    @Override
    public String getTargetTopic(String dbSchema, String tableName) {
        List<String> topics = topicProvider.provideTopics(dbSchema, tableName);
        if (topics != null && !topics.isEmpty()) {
            return topics.get(0);
        }
        return null;
    }

    private Producer<String, String> createProducer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
        props.setProperty("client.id", this.topologyId + "_writer_" + context.getThisTaskId());

        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private BoltCommandHandlerProvider buildProvider() throws Exception {
        /*Set<Class<?>> classes = AnnotationScanner.scan("com.creditease.dbus", BoltCmdHandlerProvider.class, (clazz, annotation) -> {
            BoltCmdHandlerProvider ca = clazz.getAnnotation(BoltCmdHandlerProvider.class);
            return ca.type() == BoltType.KAFKA_WRITER_BOLT;
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
        if (type == DbusDatasourceType.ORACLE) {
            name = "com.creditease.dbus.stream.oracle.appender.bolt.processor.provider.KafkaWriterCmdHandlerProvider";
        } else if (type == DbusDatasourceType.MYSQL) {
            name = "com.creditease.dbus.stream.mysql.appender.bolt.processor.provider.KafkaWriterCmdHandlerProvider";
        } else if (type == DbusDatasourceType.MONGO) {
            name = "com.creditease.dbus.stream.mongo.appender.bolt.processor.provider.KafkaWriterCmdHandlerProvider";
        } else if (type == DbusDatasourceType.DB2) {
            name = "com.creditease.dbus.stream.db2.appender.bolt.processor.provider.KafkaWriterCmdHandlerProvider";
        } else {
            throw new IllegalArgumentException("Illegal argument [" + type.toString() + "] for building BoltCommandHandler map!");
        }
        Class<?> clazz = Class.forName(name);
        Constructor<?> constructor = clazz.getConstructor(KafkaBoltHandlerListener.class, TableMessageStatReporter.class);
        return (BoltCommandHandlerProvider) constructor.newInstance(this, tableStatReporter);
    }

    private class KafkaStatSender implements StatSender {
        @Override
        public void sendStat(String message, Object... args) {
            String key = Joiner.on(".").join(args);
            String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.DBUS_STATISTIC_TOPIC);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Encounter error while writing statics message. topic:{}, key:{}, message:{}", topic, key, message, exception);
                } else {
                    logger.debug("[heartbeat] Writing statics message. topic:{}, key:{}, message:{}", topic, key, message);
                }
            });
        }
    }
}
