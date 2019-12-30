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


package com.creditease.dbus.stream.mongo.appender.spout.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import com.creditease.dbus.stream.mongo.appender.convertor.Convertor;
import com.creditease.dbus.stream.mongo.appender.parser.OplogParser;
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

/**
 * 处理拉全量事件的processor
 * Created by ximeiwang on 2017/12/19.
 */
public class MongoInitialLoadProcessor extends AbstractProcessor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final OplogParser parser;
    private List<String> controlTopics;
    private Cache<String, Object> cache;

    public MongoInitialLoadProcessor(List<String> ctrlTopics, RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        this.controlTopics = ctrlTopics;
        cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
        //TODO 解析tuple中的entry信息
        parser = OplogParser.getInstance();
    }

    /**
     * 处理拉全量请求
     */
    @Override
    //public void process(ConsumerRecord<String, byte[]> record, Object... args) {
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        try {
            //List<String> msgList = parser.getEntry(record.value());
            String entry = parser.getEntry(record.value());
            if (entry.isEmpty())
                return;

            JSONObject entryJson = JSON.parseObject(entry);
            String operation = entryJson.get("_op").toString();
            //TODO 暂时放弃拉全量表的update/delete等消息
            if (!operation.toLowerCase().equals("i")) {
                listener.reduceFlowSize(record.serializedValueSize());
                consumerListener.syncOffset(record);
                return;
            }

            // 判断是否处理过该消息
            String msgId = entryJson.getString("_ts");
            Object processed = cache.getIfPresent(msgId);
            if (processed != null) {
                logger.info("Data have bean processed, data position is [{}]", msgId);
                listener.reduceFlowSize(record.serializedValueSize());
                consumerListener.syncOffset(record);
                return;
            }

            logger.info("Received FULL DATA PULL REQUEST message");
            ControlMessage message = Convertor.mongoFullPullMessage(entry, listener.getListenerId(), record);

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
                future.get(); //TODO 这里可以用future.get吗？
                logger.info("write initial load request message to kafka: {}", json);
            }

            // 暂停接收 topic 的数据
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            consumerListener.pauseTopic(tp, record.offset(), message);

            // emit FULL_DATA_PULL_REQ 通知给bolt
            EmitData emitData = new EmitData();
            emitData.add(EmitData.DB_SCHEMA, schemaName);
            emitData.add(EmitData.DATA_TABLE, tableName);

            //List<Object> values = new Values(emitData, Command.FULL_DATA_PULL_REQ);
            listener.emitData(emitData, Command.FULL_DATA_PULL_REQ, record);

            consumerListener.syncOffset(record); //TODO 这个要加吗？
            cache.put(msgId, entry);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getStringValue(String key, ControlMessage message) {
        return message.payloadValue(key, Object.class).toString();
    }
/*
    @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {

    }*/
}
