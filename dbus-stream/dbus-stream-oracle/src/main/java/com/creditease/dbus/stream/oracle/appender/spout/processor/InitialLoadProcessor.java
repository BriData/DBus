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


package com.creditease.dbus.stream.oracle.appender.spout.processor;

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
import com.creditease.dbus.stream.common.appender.utils.ControlMessageEncoder;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.oracle.appender.avro.GenericData;
import com.creditease.dbus.stream.oracle.appender.avro.GenericSchemaDecoder;
import com.creditease.dbus.stream.oracle.appender.avro.SchemaProvider;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
 * Created by Shrimp on 16/6/21.
 */
public class InitialLoadProcessor extends AbstractProcessor {

    private final GenericSchemaDecoder decoder;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<String> controlTopics;
    private Cache<String, Object> cache;

    public InitialLoadProcessor(List<String> ctrlTopics, RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        this.controlTopics = ctrlTopics;
        cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
        this.decoder = GenericSchemaDecoder.decoder();
    }

    /**
     * 处理拉全量请求
     *
     * @param consumerRecord kafka record对象
     * @param args           数据参数
     * @throws Exception
     */
    public void process(final DBusConsumerRecord<String, byte[]> consumerRecord, Object... args) {

        try {
            List<GenericData> schemaList = decoder.unwrap(consumerRecord.value());
            if (schemaList.isEmpty()) {
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                return;
            }

            // 拉全量请求已经被拆分成独立的record,不会出现unwrap出来多条记录的情况
            GenericData data = schemaList.get(0);
            String schemaName = Utils.buildAvroSchemaName(data.getSchemaHash(), data.getTableName());
            Schema schema = SchemaProvider.getInstance().getSchema(schemaName);
            List<GenericRecord> records = decoder.decode(schema, data.getPayload());

            if (records != null && records.isEmpty()) {
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                consumerListener.syncOffset(consumerRecord);
                return;
            }

            GenericRecord record = records.get(0);
            String optype = record.get(Constants.MessageBodyKey.OP_TYPE).toString();
            //TODO 暂时放弃拉全量表的update/delete等消息
            if (!Constants.OperationTypes.INSERT.equalsIgnoreCase(optype)) {
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                consumerListener.syncOffset(consumerRecord);
                return;
            }

            // 判断是否处理过该消息
            Object processed = cache.getIfPresent(record.get(Constants.MessageBodyKey.POS).toString());
            if (processed != null) {
                logger.info("Data have bean processed, the data position in osource is [{}]", record.get(Constants.MessageBodyKey.POS));
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                consumerListener.syncOffset(consumerRecord);
                return;
            }


            logger.info("Received FULL DATA PULL REQUEST message");
            ControlMessage message = ControlMessageEncoder.fullDataPollMessage(record, listener.getListenerId(), consumerRecord);
            String dbschema = getStringValue("SCHEMA_NAME", message);
            String dataTable = getStringValue("TABLE_NAME", message);
            DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(dbschema, dataTable));
            if (table == null) {
                listener.reduceFlowSize(consumerRecord.serializedValueSize());
                logger.warn("Table {}.{} is not supported,please configure it in dbus database.", dbschema, dataTable);
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
            emitData.add(EmitData.DB_SCHEMA, dbschema);
            emitData.add(EmitData.DATA_TABLE, dataTable);

            listener.emitData(emitData, Command.FULL_DATA_PULL_REQ, consumerRecord);

            //consumerListener.syncOffset(consumerRecord);
            cache.put(record.get("pos").toString(), record.toString());
        } catch (Exception e) {
            listener.reduceFlowSize(consumerRecord.serializedValueSize());
            throw new RuntimeException(e);
        }
    }

    private String getStringValue(String key, ControlMessage message) {
        return message.payloadValue(key, Object.class).toString();
    }
}
