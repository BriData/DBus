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


package com.creditease.dbus.stream.common.appender.bolt.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.commons.meta.MetaCompareResult;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyf on 17/2/14.
 */
public class MetaEventWarningSender {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Producer<String, String> producer;

    public MetaEventWarningSender() {
        try {
            producer = createProducer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sendMessage(MetaVersion ver, MetaWrapper newMeta, MetaCompareResult result) {
        ControlMessage message = new ControlMessage(System.currentTimeMillis(), ControlType.G_META_SYNC_WARNING.toString(), "dbus-appender");

        message.addPayload("datasource", GlobalCache.getDatasource().getDsName());
        message.addPayload("schema", ver.getSchema());
        message.addPayload("tableId", ver.getTableId());
        message.addPayload("table", ver.getTable());
        message.addPayload("before", ver.getMeta());
        message.addPayload("after", newMeta);
        message.addPayload("compare-result", JSON.toJSON(result));
        message.addPayload("version", ver.getVersion());

        String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.GLOBAL_EVENT_TOPIC);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getType(), message.toJSONString());
        Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Send global event error.{}", exception.getMessage());
            }
        });
        try {
            future.get(10000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void sendMaasAppenderMessage(MaasAppenderMessage maasAppenderMessage) {
        ControlMessage message = new ControlMessage(System.currentTimeMillis(), ControlType.G_MAAS_APPENDER_EVENT.toString(), "dbus-appender");

        message.setPayload(JSONObject.parseObject(maasAppenderMessage.toString()));

        String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.GLOBAL_EVENT_TOPIC);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getType(), message.toJSONString());
        Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Send global event error.{}", exception.getMessage());
            }
        });
        try {
            future.get(10000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static Producer<String, String> createProducer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
        //props.setProperty("client.id", "meta-event-warning");

        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            if (!(e instanceof InstanceAlreadyExistsException)) {
                throw e;
            }
        }
        return producer;
    }
}
