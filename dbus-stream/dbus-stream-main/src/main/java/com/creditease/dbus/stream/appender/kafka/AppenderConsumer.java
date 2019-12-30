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


package com.creditease.dbus.stream.appender.kafka;

import avro.shaded.com.google.common.collect.Lists;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.appender.utils.JsonNodeOperator;
import com.creditease.dbus.stream.appender.utils.ZKNodeOperator;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.CtrlMessagePostOperation;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Shrimp on 16/8/19.
 */
public class AppenderConsumer implements ConsumerListener, Closeable {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private int taskId;
    private IZkService zk;
    private String zkconnect;
    private String topologyId;
    private List<String> dataTopics;
    private List<String> schemaTopics;
    private List<String> controlTopics;
    private Map<String, TopicInfo> pausedTopics;
    private Consumer<String, byte[]> consumer;
    public ConsumerProvider<String, byte[]> consumerProvider;
    private ZKNodeOperator offsetOperator;
    private ZKNodeOperator zkNodeOperator;

    public AppenderConsumer(String topologyId, int taskId, String zkconnect) throws Exception {
        this.taskId = taskId;
        this.topologyId = topologyId;
        this.pausedTopics = new HashMap<>();
        this.zkconnect = zkconnect;

        this.zk = new ZkService(zkconnect);
        offsetOperator = new JsonNodeOperator(zk, Utils.join("/", Utils.buildZKTopologyPath(topologyId), Constants.ZKPath.SPOUT_KAFKA_CONSUMER_NEXTOFFSET));
        offsetOperator.createWhenNotExists(Collections.emptyMap());

        zkNodeOperator = new JsonNodeOperator(zk, Utils.join("/", Utils.buildZKTopologyPath(topologyId), Constants.ZKPath.ZK_FULLDATA_REQ_PARTITIONS));
        zkNodeOperator.createWhenNotExists(Collections.emptyMap());
        this.consumerProvider = new SpoutConsumerProvider(offsetOperator);

        this.consumer = createConsumer();
    }

    public ConsumerRecords<String, byte[]> getMessages() {
        return consumer.poll(0);
    }

    private Consumer<String, byte[]> createConsumer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.CONSUMER_CONFIG);

        props.setProperty("client.id", this.topologyId + "_consumer");
        props.setProperty("group.id", this.topologyId + "_grp");

        TopicProvider dataTopicProvider = new DataInputTopicProvider();
        TopicProvider controlTopicProvider = new ControlTopicProvider();
        TopicProvider schemaTopicProvider = new SchemaTopicProvider();
        this.controlTopics = controlTopicProvider.provideTopics();
        this.dataTopics = dataTopicProvider.provideTopics();

        List<String> topics = Lists.newArrayList();
        topics.addAll(controlTopics);
        topics.addAll(dataTopics);

        if (DbusDatasourceType.ORACLE == GlobalCache.getDatasourceType()
                || DbusDatasourceType.DB2 == GlobalCache.getDatasourceType()
        ) {
            this.schemaTopics = schemaTopicProvider.provideTopics();
            topics.addAll(schemaTopics);
        }
        Consumer<String, byte[]> consumer = consumerProvider.consumer(props, topics);

        Map<String, Object> data = zkNodeOperator.getData();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            TopicInfo b = TopicInfo.build((Map<String, Object>) entry.getValue());
            this.pausedTopics.put(b.getTopic(), b);
        }

        consumerProvider.pause(consumer, this.pausedTopics.entrySet()
                .stream()
                .map(entry -> entry.getValue())
                .collect(Collectors.toList()));

        return consumer;
    }

    @Override
    public void pauseTopic(TopicPartition tp, long offset, ControlMessage message) {

        String topic = message.payloadValue("topic", String.class);
        String tableName = message.payloadValue("TABLE_NAME", String.class);
        if (topic == null || topic.length() == 0) topic = tp.topic();
        if (!pausedTopics.containsKey(topic)) {
            consumer.pause(Arrays.asList(tp));
            TopicInfo topicInfo = TopicInfo.build(tp.topic(), tp.partition(), offset, tableName);
            pausedTopics.put(topic, topicInfo);

            try {
                zkNodeOperator.setData(pausedTopics, true);
            } catch (Exception e) {
                logger.error("Adding paused topics error", e);
            }
            logger.info("Topic [{}] was paused by command", tp.topic());
        } else {
            logger.info("Topic [{}] has been paused, the pause action was skipped", tp.topic());
        }
    }

    @Override
    public void resumeTopic(String topic, ControlMessage message) {
        String result = null;
        if (pausedTopics.containsKey(topic)) {
            TopicInfo b = pausedTopics.get(topic);
            TopicPartition tp = b.partition();
            consumer.resume(Arrays.asList(tp));

            // 如果consumer.poll()获取到多条schema1的数据形式如下:
            // schema1.offset2->schema1.offset1->schema1.offset_full_req
            // 此时这三条数据都会被处理,并且commit offset,即处理拉全量完成后
            // 执行resume时会从schema1.offset3的下一条开始读取数据,这样会丢失
            // schema1.offset2和schema1.offset1两条数据,所以要seek到offset_full_req的下一条数据
            // 的offset,即schema1.offset_full_req + 1
            consumer.seek(tp, b.getOffset() + 1);

            pausedTopics.remove(topic);
            try {
                zkNodeOperator.setData(pausedTopics, true);
                result = "ok";
            } catch (Exception e) {
                result = "Removing paused topics error. Msg: " + e.getMessage();
                logger.error("Removing paused topics error", e);
            }
            logger.info("Topic [{}] was resumed by command", tp.topic());
        } else {
            logger.info("No paused topic was found by key: {}", topic);
            result = "No paused topic was found by key:" + topic;
        }
        CtrlMessagePostOperation oper = CtrlMessagePostOperation.create(zkconnect);
        oper.spoutResumed(message, result);
    }

    @Override
    public void syncOffset(DBusConsumerRecord<String, byte[]> record) {
        logger.debug("Ack offset {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset() + 1);
        final String topic = record.topic();
        final int partition = record.partition();
        final long offset = record.offset();
        Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, ""));
        consumer.commitAsync(map, (metadata, exception) -> {
            if (exception != null) {
                logger.error("[appender-consumer] commit error [topic:{},partition:{},offset:{}]., error message: {}", topic, partition, offset, exception.getMessage());
            }
        });
    }

    @Override
    public void pauseAppender() {
        List<TopicPartition> topics = this.dataTopics.stream().map(topic -> new TopicPartition(topic, 0)).collect(Collectors.toList());
        consumer.pause(topics);
    }

    @Override
    public void resumeAppender() {
        List<TopicPartition> topics = this.dataTopics.stream().map(topic -> new TopicPartition(topic, 0)).collect(Collectors.toList());
        consumer.resume(topics);
    }

    @Override
    public boolean filterPausedTopic(String topic) {
        return !pausedTopics.containsKey(topic);
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    @Override
    public void seek(DBusConsumerRecord<String, byte[]> record) {
        logger.info("seek topic {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset());
        consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    @Override
    public List<String> getControlTopics() {
        return controlTopics;
    }

    @Override
    public List<String> getDataTopics() {
        return dataTopics;
    }

    @Override
    public List<String> getSchemaTopics() {
        return schemaTopics;
    }
}
