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


package com.creditease.dbus.stream.common.appender.spout.processor;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.Constants;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * spout消息处理器的抽象,以及具体消息处理器简单工厂的实现
 * Created by Shrimp on 16/8/22.
 */
public abstract class AbstractMessageHandler {
    private static Logger logger = LoggerFactory.getLogger(AbstractMessageHandler.class);

    protected List<String> controlTopics;
    protected String initialLoadTableNs;
    protected RecordProcessListener listener;
    protected ConsumerListener consumerListener;

    public AbstractMessageHandler(RecordProcessListener rpListener, ConsumerListener consumerListener) {
        this.listener = rpListener;
        this.consumerListener = consumerListener;
        this.controlTopics = Lists.newArrayList(consumerListener.getControlTopics());
        this.initialLoadTableNs = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE,
                Constants.ConfigureKey.FULLDATA_REQUEST_SRC);
    }

    /**
     * 根据datasource type 生成相应的消息处理器,如果类型错误则会抛出 IllegalArgumentException
     *
     * @param datasourceType   DbusDatasourceType 枚举
     * @param rpListener       和spout通信的监听器
     * @param consumerListener 和AppenderConsumer通信的监听器
     * @return 返回消息处理器实现
     */
    public static AbstractMessageHandler newInstance(DbusDatasourceType datasourceType,
                                                     RecordProcessListener rpListener,
                                                     ConsumerListener consumerListener) {
        /*Set<Class<?>> clazzSet = AnnotationScanner.scan("com.creditease.dbus", SpoutMessageHandler.class);

        if (clazzSet.size() == 0) {
            throw new RuntimeException("No SpoutMessageHandler found.");
        }

        if (clazzSet.size() > 1) {
            throw new RuntimeException("Too many SpoutMessageHandlers found.");
        }

        Class<?> clazz = clazzSet.iterator().next();
        if (!AbstractMessageHandler.class.isAssignableFrom(clazz)) {
            throw new RuntimeException("Invalid message handler.");
        }
        logger.info("SpoutMessageHandler found:{}", clazz.getName());*/

        try {
            String name;
            if (datasourceType == DbusDatasourceType.MYSQL) {
                name = "com.creditease.dbus.stream.mysql.appender.spout.processor.MysqlMessageHandler";
            } else if (datasourceType == DbusDatasourceType.ORACLE) {
                name = "com.creditease.dbus.stream.oracle.appender.spout.processor.OracleMessageHandler";
            } else if (datasourceType == DbusDatasourceType.MONGO) {
                name = "com.creditease.dbus.stream.mongo.appender.spout.processor.MongoMessageHandler";
            } else if (datasourceType == DbusDatasourceType.DB2) {
                name = "com.creditease.dbus.stream.db2.appender.spout.processor.Db2MessageHandler";
            } else {
                throw new IllegalArgumentException(datasourceType.toString() + " not support.");
            }
            Class<?> clazz = Class.forName(name);
            Constructor<?> constructor = clazz.getConstructor(RecordProcessListener.class, ConsumerListener.class);
            return (AbstractMessageHandler) constructor.newInstance(rpListener, consumerListener);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 选择相应的Processor处理器,用以处理ConsumerRecords<String, byte[]>类型的RecordProcessor实现
     *
     * @param recordKey ConsumerRecords<String, byte[]>的key值
     * @param topic     ConsumerRecords<String, byte[]>所在的topic
     * @return 返回具体RecordProcessor
     */
    protected abstract RecordProcessor<String, byte[]> chooseProcessor(String recordKey, String topic);

    /**
     * 处理通过kafka consumer获取到的一批消息
     *
     * @param records 待处理的消息集合
     */
    public void handleMessages(ConsumerRecords<String, byte[]> records) {

        // 按记录进行处理
        for (ConsumerRecord<String, byte[]> record : records) {
            // 过滤掉暂停的topic的消息
            if (consumerListener.filterPausedTopic(record.topic())) {
                listener.increaseFlowSize(record.serializedValueSize());
                try {
                    this.chooseProcessor(record.key(), record.topic()).process(new DBusConsumerRecord<String, byte[]>(record));
                } catch (Exception e) {
                    logger.error("sport process error", e);
                    consumerListener.seek(new DBusConsumerRecord<String, byte[]>(record));
                    break;
                }
            } else {
                listener.reduceFlowSize(record.serializedValueSize());
                consumerListener.syncOffset(new DBusConsumerRecord<String, byte[]>(record));
                logger.info("The record of topic {} was skipped whose offset is {}", record.topic(), record.offset());
            }
        }
    }
}
