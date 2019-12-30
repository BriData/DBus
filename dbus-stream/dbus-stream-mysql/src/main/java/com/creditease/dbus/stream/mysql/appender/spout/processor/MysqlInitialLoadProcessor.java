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


package com.creditease.dbus.stream.mysql.appender.spout.processor;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractProcessor;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.protobuf.convertor.Convertor;
import com.creditease.dbus.stream.mysql.appender.protobuf.parser.BinlogProtobufParser;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MysqlInitialLoadProcessor extends AbstractProcessor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final BinlogProtobufParser parser;
    private List<String> controlTopics;
    private Cache<String, Object> cache;

    public MysqlInitialLoadProcessor(List<String> ctrlTopics, RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        this.controlTopics = ctrlTopics;
        cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
        parser = BinlogProtobufParser.getInstance();
    }

    /**
     * 处理mysql 拉全量请求
     */
    @Override
    public void process(final DBusConsumerRecord<String, byte[]> consumerRecord, Object... args) {
        try {
            List<MessageEntry> msgEntryLst = parser.getEntry(consumerRecord.value());
            if (msgEntryLst.isEmpty())
                return;
            EntryHeader entryHeader = msgEntryLst.get(0).getEntryHeader();
            EventType operType = entryHeader.getOperType();

            //TODO 暂时放弃拉全量表的update/delete等消息
            if (operType != EventType.INSERT) {
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                consumerListener.syncOffset(consumerRecord);
                return;
            }

            // 判断是否处理过该消息
            String msgPos = entryHeader.getPos();
            Object processed = cache.getIfPresent(msgPos);
            if (processed != null) {
                logger.info("Data have bean processed, the data position is [{}]", msgPos);
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                consumerListener.syncOffset(consumerRecord);
                return;
            }

            logger.info("Received FULL DATA PULL REQUEST message");
            ControlMessage message = Convertor.mysqlFullPullMessage(msgEntryLst.get(0), listener.getListenerId(), consumerRecord);
            String schemaName = getStringValue("SCHEMA_NAME", message);
            String tableName = getStringValue("TABLE_NAME", message);
            DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(schemaName, tableName));
            if (table == null) {
                logger.warn("Table {}.{} is not supported,please configure it in dbus database.", schemaName, tableName);
                return;
            }

            for (String controlTopic : controlTopics) {
                String json = message.toJSONString();
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(controlTopic, message.getType(), json.getBytes());

                Future<RecordMetadata> future = listener.sendRecord(producerRecord);
                future.get();

                logger.info("write initial load request message to kafka: {}", json);
            }

            // 暂停接收 topic 的数据
            TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            consumerListener.pauseTopic(tp, consumerRecord.offset(), message);

            // emit FULL_DATA_PULL_REQ 通知给bolt
            EmitData emitData = new EmitData();
            emitData.add(EmitData.DB_SCHEMA, schemaName);
            emitData.add(EmitData.DATA_TABLE, tableName);
            listener.emitData(emitData, Command.FULL_DATA_PULL_REQ, consumerRecord);

            consumerListener.syncOffset(consumerRecord);
            cache.put(msgPos, msgEntryLst.get(0).toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getStringValue(String key, ControlMessage message) {
        return message.payloadValue(key, Object.class).toString();
    }
}
