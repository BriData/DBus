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


package com.creditease.dbus.stream.appender.spout;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.ZkContentHolder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.appender.kafka.AppenderConsumer;
import com.creditease.dbus.stream.appender.spout.queue.MessageStatusQueueManager;
import com.creditease.dbus.stream.appender.utils.MetaVersionInitializer;
import com.creditease.dbus.stream.appender.utils.ReloadStatus;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.metrics.CountMetricReporter;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractMessageHandler;
import com.creditease.dbus.stream.common.appender.spout.processor.CtrlMessagePostOperation;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.creditease.dbus.stream.common.Constants.*;

/**
 * 读取kafka数据的Spout实现
 * dbus-dispatcher 会将表的数据拆分到所有其他schema所对应的kafka topic中
 * Created by Shrimp on 16/6/2.
 */
public class DbusKafkaSpout extends BaseRichSpout implements RecordProcessListener {

    private Logger logger = LoggerFactory.getLogger(DbusKafkaSpout.class);

    private TopologyContext context;
    private String topologyId;
    private String zkconnect;
    private String datasource;
    private String zkRoot;
    private boolean initialized = false;
    private int rCount = 0;
    private int MAX_FLOW_THRESHOLD;
    private int flowBytes = 0;

    private AppenderConsumer consumer;
    private SpoutOutputCollector collector;
    private Producer<String, byte[]> producer;
    private MetaVersionInitializer mvInitializer;
    private DbusDatasourceType datasourceType;
    private AbstractMessageHandler messageHandler;
    private ReloadStatus status;

    private MessageStatusQueueManager msgQueueMgr;

    private CountMetricReporter countMetricReporter;
//    private String initialLoadTableNs;
//    private String metaEventTableNs;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.context = context;
        this.collector = collector;
        this.topologyId = (String) conf.get(StormConfigKey.TOPOLOGY_ID);
        this.datasource = (String) conf.get(StormConfigKey.DATASOURCE);
        this.zkconnect = (String) conf.get(StormConfigKey.ZKCONNECT);
        this.zkRoot = Utils.buildZKTopologyPath(topologyId);

        countMetricReporter = CountMetricReporter.create("appender-spout");
        // consumer 订阅的topic列表
        if (!initialized) {
            try {
                // 初始化配置文件
                PropertiesHolder.initialize(zkconnect, zkRoot);
                ZkContentHolder.initialize(zkconnect, zkRoot);
                GlobalCache.initialize(datasource);
                mvInitializer = new MetaVersionInitializer();
                status = new ReloadStatus();
                reload();

                logger.info(getClass().getName() + " Initialized!");
            } catch (Exception e) {
                logger.error("Sport initialize error", e);
                throw new InitializationException(e);
            }
            initialized = true;
        }
    }

    @Override
    public void nextTuple() {
        if (!reloadSpout()) return;  // 判断是否重新加载了缓存,如果重新加载则直接返回
        if (flowLimitation()) return; // 如果读取的流量过大则要sleep一下
        // 读取kafka消息
        ConsumerRecords<String, byte[]> records = consumer.getMessages();

        if (records.isEmpty()) {
            bubble();
            return;
        }

        //TODO 判断是否为事件，如果消息为事件则需要等待消息队列中的数据处理完之后再处理,
        //TODO 由于mysql需要将消息数据完全解析完成后才能获取是否为表结构变更事件，暂时不实现
        /*Set<String> set = new HashSet<>();
        // 判断是否为事件，如果消息为事件则需要等待消息队列中的数据处理完之后再处理
        Iterator<ConsumerRecord<String, byte[]>> it = records.iterator();
        List<ConsumerRecord<String, byte[]>> list = new ArrayList<>();
        while (it.hasNext()) {
            ConsumerRecord<String, byte[]> record = it.next();

            // 记录在set中的topic partition的消息将本次将不会被处理
            if (set.contains(record.topic() + record.partition())) {
                continue;
            }

            // 验证是否为事件/判断队列中是否包含待处理的消息（需要区分topic和partition）
            if (isEvent(record) && !msgQueueMgr.isAllMessageProcessed(record)) {
                // 如果队列中还有待处理的消息，则需要等待队列中所有消息处理完之后才能处理该事件，
                // 实现等待的方式：seek到record所在的位置
                consumer.seek(record);
                logger.info("received an event[{}], seek to [topic:{},partition:{},offset:{}] to " +
                        "waiting until messages processed in the queue.", record.key(), record.topic(), record.partition(), record.offset());

                // 记录事件产生的topic和和partition，
                // records记录列表中在record之后的消息(和record的topic和partition相同的消息)
                // 将不会被：messageHandler.handleMessages(records)处理
                set.add(record.topic() + record.partition());
            } else {
                list.add(record);
            }

        }
        if (!list.isEmpty()) {
            messageHandler.handleMessages(list);
        }*/

        messageHandler.handleMessages(records);
    }

    private Producer<String, byte[]> createProducer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONTROL);
        props.setProperty("client.id", this.topologyId + "_control_" + context.getThisTaskId());

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        return producer;
    }

    private boolean reloadSpout() {
        // 重新加载配置文件
        if (status.isReadyToReload()) {
            logger.info("Ready to reload spout");
            try {
                reload();
                status.reloaded();
                logger.info("Spout reloaded!");

                // 将reload完成的消息发送到zk
                CtrlMessagePostOperation oper = CtrlMessagePostOperation.create(zkconnect);
                oper.spoutReloaded(status.getExtParam("message"));

                EmitData data = new EmitData();
                data.add(EmitData.MESSAGE, status.getExtParam("message"));
                List<Object> values = new Values(data, Command.APPENDER_RELOAD_CONFIG);

                this.collector.emit(values); // 不需要跟踪消息的处理状态，即不会调用ack或者fail
            } catch (Exception e) {
                logger.error("Spout reload error!", e);
            }
            return true;
        }
        return true;
    }

    private void reload() throws Exception {

        PropertiesHolder.reload();
        // 加载缓存
        GlobalCache.refreshCache();
        ThreadLocalCache.reload();

        datasourceType = GlobalCache.getDatasourceType();
        //TODO 在spout中初始化 MetaVersionInitializer 并不合适, 应该在web端管理功能完善后转移到web端添加数据源时初始化
        mvInitializer.initialize(datasourceType);

        // 重新初始化Command
        Command.initialize();

        if (this.consumer != null) {
            consumer.close();
        }
        this.consumer = new AppenderConsumer(topologyId, context.getThisTaskId(), zkconnect);

        messageHandler = AbstractMessageHandler.newInstance(datasourceType, this, consumer);

        if (this.producer != null) {
            this.producer.close();
        }
        this.producer = createProducer();
        this.msgQueueMgr = new MessageStatusQueueManager();
        this.MAX_FLOW_THRESHOLD = PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, ConfigureKey.SPOUT_MAX_FLOW_THRESHOLD);
//        this.initialLoadTableNs = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.FULLDATA_REQUEST_SRC);
//        this.metaEventTableNs = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, ConfigureKey.META_EVENT_SRC);
    }

    @Override
    public void emitData(EmitData data, Command cmd, Object msgId) {
        countMetricReporter.mark();
        List<Object> values = new Values(data, cmd);
        if (msgId != null && msgId instanceof DBusConsumerRecord) {
            DBusConsumerRecord<String, byte[]> record = getMessageId(msgId);
            msgQueueMgr.addMessage(record);
            this.collector.emit(values, cloneWithoutValue(record));
            logger.info("[dbus-spout-emit] topic: {}, offset: {}, key: {}, size: {}", record.topic(), record.offset(), record.key(), record.value().length);
        } else {
            this.collector.emit(values, msgId);
        }
    }


    private DBusConsumerRecord<String, byte[]> cloneWithoutValue(DBusConsumerRecord<String, byte[]> record) {
        DBusConsumerRecord<String, byte[]> r = new DBusConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(),
                record.checksum(), record.serializedKeySize(), record.value().length, record.key(), null);
        return r;
    }

    @Override
    public Future<RecordMetadata> sendRecord(ProducerRecord<String, byte[]> producerRecord) {
        return producer.send(producerRecord);
    }

    @Override
    public void markReloading(DBusConsumerRecord<String, byte[]> record, Map<String, Object> params) {
        status.markReloading(record);
        status.addAllExts(params);
    }

    @Override
    public String getListenerId() {
        return topologyId;
    }

    @Override
    public void reduceFlowSize(int recordSize) {
        this.flowBytes -= recordSize;
        logger.debug("reduce flow size:{}", this.flowBytes);
    }

    @Override
    public void increaseFlowSize(int recordSize) {
        this.flowBytes += recordSize;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EmitFields.DATA, EmitFields.COMMAND));
    }

    @Override
    public void ack(Object msgId) {
        countMetricReporter.mark(-1);
        if (msgId != null && DBusConsumerRecord.class.isInstance(msgId)) {
            DBusConsumerRecord<String, byte[]> record = getMessageId(msgId);
            reduceFlowSize(record.serializedValueSize());

            // 标记处理成功并获取commit点
            DBusConsumerRecord<String, byte[]> commitPoint = msgQueueMgr.okAndGetCommitPoint(record);
            if (commitPoint != null) {
                consumer.syncOffset(commitPoint);
                msgQueueMgr.committed(commitPoint);
            }
            logger.info("[dbus-spout-ack] topic: {}, offset: {}, key: {}, size:{}, sync-kafka-offset:{}", record.topic(),
                    record.offset(), record.key(), record.serializedValueSize(), commitPoint != null ? commitPoint.offset() : -1);
        } else {
            logger.info("[dbus-spout-ack] receive ack message[{}]", msgId.getClass().getName());
        }
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        countMetricReporter.mark(-1);
        if (msgId != null && DBusConsumerRecord.class.isInstance(msgId)) {
            DBusConsumerRecord<String, byte[]> record = getMessageId(msgId);
            logger.error("[dbus-spout-fail] topic: {}, offset: {}, key: {}, size: {}", record.topic(), record.offset(), record.key(), record.serializedValueSize());
            this.reduceFlowSize(record.serializedValueSize());
            // 如果是拉全量处理出错,则需要将暂停的topic唤醒
            if (Utils.parseCommand(record.key()) == Command.FULL_DATA_PULL_REQ) {
                ControlMessage message;
                try {
                    message = ControlMessage.parse(new String(record.value(), "utf-8"));
                } catch (UnsupportedEncodingException e) {
                    message = new ControlMessage();
                    message.setId(System.currentTimeMillis());
                    message.setType(Command.FULL_DATA_PULL_REQ.toString());
                    logger.warn(e.getMessage(), e);
                }
                consumer.resumeTopic(record.topic(), message);
                logger.warn("Spout receive 'fail' when process FULL_DATA_PULL_REQ. Resume to read data from topic {}", record.topic());
            }
            this.reduceFlowSize(record.serializedValueSize());

            // 标记处理失败,获取seek点
            DBusConsumerRecord<String, byte[]> seekPoint = msgQueueMgr.failAndGetSeekPoint(record);
            if (seekPoint != null) {
                consumer.seek(seekPoint);
            }
        } else {
            logger.info("[dbus-spout-fail] receive ack message[{}]", msgId.getClass().getName());
        }
        super.fail(msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void close() {
        try {
            consumer.close();
            super.close();
        } catch (IOException e) {
            logger.error("Close consumer error!", e);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    private boolean flowLimitation() {
        if (flowBytes >= MAX_FLOW_THRESHOLD) {
            logger.info("Flow control: Spout gets {} bytes data.", flowBytes);
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            return true;
        }
        return false;
    }

    private void bubble() {
        rCount++;
        if (rCount == 10000) {
            logger.info("Spout running...");
            rCount = 0;
            flowBytes = 0; // 容错,避免出现数据处理完成或者失败,没有成功的减少flowBytes数的情况导致实际流量变小的问题
        }
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }
    }


    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }

    /*private boolean isEvent(ConsumerRecord<String, byte[]> record) {
        String key = record.key();
        boolean result = false;
        if (key != null) {
            result = key.startsWith(initialLoadTableNs) || key.startsWith(metaEventTableNs);
        }
        return result;
    }*/

    public static void main(String[] args) {
        System.out.println(ConsumerRecord.class.isInstance(null));
    }
}
