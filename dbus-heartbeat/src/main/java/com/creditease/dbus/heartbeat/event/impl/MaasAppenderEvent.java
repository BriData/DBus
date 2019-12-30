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


package com.creditease.dbus.heartbeat.event.impl;


import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.MaasAppenderMessage;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.IDbusDataDao;
import com.creditease.dbus.heartbeat.dao.impl.DbusDataDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.type.MaasMessage;
import com.creditease.dbus.heartbeat.type.MaasMessage.DataSource;
import com.creditease.dbus.heartbeat.type.MaasMessage.SchemaInfo;
import com.creditease.dbus.heartbeat.type.MaasMessage.SchemaInfo.Column;
import com.creditease.dbus.heartbeat.util.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by dashencui on 2017/9/19.
 */
public class MaasAppenderEvent extends AbstractEvent {

    private int waitingTimes = 0;

    private IDbusDataDao dao;

    protected String topic;

    protected String dataTopic;

    protected Consumer<String, String> dataConsumer = null;

    protected Producer<String, String> statProducer = null;

    protected TopicPartition partition0 = null;


    public MaasAppenderEvent(String topic, String dataTopic) {
        super(01);
        this.topic = topic;
        this.dataTopic = dataTopic;
        dao = new DbusDataDaoImpl();
        Properties props = HeartBeatConfigContainer.getInstance().getKafkaConsumerConfig();
        Properties producerProps = HeartBeatConfigContainer.getInstance().getmaasConf().getProducerProp();
        try {
            LoggerFactory.getLogger().info("[topic]   ...." + topic);
            LoggerFactory.getLogger().info("[maas-appender-event]  initial.........................");
            dataConsumer = new KafkaConsumer<>(props);
            partition0 = new TopicPartition(this.topic, 0);
            dataConsumer.assign(Arrays.asList(partition0));
            dataConsumer.seekToEnd(Arrays.asList(partition0));

            statProducer = new KafkaProducer<>(producerProps);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String key = null;
        String value = null;
        try {
            while (isRun.get()) {
                try {
                    ConsumerRecords<String, String> records = dataConsumer.poll(1000);
                    if (records.isEmpty()) {
                        waitingTimes++;
                        if (waitingTimes >= 30) {
                            waitingTimes = 0;
                            boolean is_success = dao.testQuery(Constants.CONFIG_DB_KEY);
                            LoggerFactory.getLogger().info("[maas-event]  connection is success: {}", is_success);
                            LoggerFactory.getLogger().info("[MaasAppenderEvent]  polling ....");
                        }
                        continue;
                    }
                    waitingTimes = 0;

                    for (ConsumerRecord<String, String> record : records) {
                        key = record.key();
                        value = record.value();

                        LoggerFactory.getLogger().info("[MaasAppenderEvent]  topic:{}, key:{}, value:{}", topic, key, value);
                        if (StringUtils.isEmpty(value)) {
                            LoggerFactory.getLogger().error("[MaasAppenderEvent]  topic:{}, value:null, offset:{}", topic, record.offset());
                            continue;
                        }

                        if (ControlType.G_MAAS_APPENDER_EVENT.toString().equals(key)) {//
                            LoggerFactory.getLogger().info("向maas发送消息到指定kafka");

                            MaasAppenderMessage maasAppenderMessage = JSON.parseObject(JSON.parseObject(value).getString("payload"), MaasAppenderMessage.class);

                            long dsId = maasAppenderMessage.getDsId();
                            String schemaName = maasAppenderMessage.getSchemaName();
                            String tableName = maasAppenderMessage.getTableName();

                            MaasMessage data_topic_message = new MaasMessage();
                            DataSource dataSource = dao.queryDsInfoUseDsId(Constants.CONFIG_DB_KEY, dsId);
                            data_topic_message.setData_source(dataSource);//统一设置数据源信息
                            data_topic_message.setObject_owner(schemaName);//统一设置属主信息

                            if (maasAppenderMessage.getType().equals(MaasAppenderMessage.Type.ALTER)) {
                                //ALTER情况，设置所有列
                                List<Column> columnList = dao.queryColumsUseMore(Constants.CONFIG_DB_KEY, dsId, schemaName, tableName);
                                String table_comment = dao.queryTableComment2(Constants.CONFIG_DB_KEY, dsId, schemaName, tableName);
                                setMessageAlter(data_topic_message, maasAppenderMessage, columnList, table_comment);
                            } else if (maasAppenderMessage.getType().equals(MaasAppenderMessage.Type.COMMENT_COLUMN)) {
                                //COMMENT_COLUMN 情况
                                String columnName = maasAppenderMessage.getColumnName();
                                Column column = dao.queryColumForAppender(Constants.CONFIG_DB_KEY, dsId, schemaName, tableName, columnName);
                                setMessageColumn(data_topic_message, maasAppenderMessage, column);

                            } else if (maasAppenderMessage.getType().equals(MaasAppenderMessage.Type.COMMENT_TABLE)) {
                                //COMMENT_TABLE 情况
                                setMessageTable(data_topic_message, maasAppenderMessage);
                            }

                            String data_message = data_topic_message.toString();
                            LoggerFactory.getLogger().info("发送的消息 " + data_message);
                            LoggerFactory.getLogger().info("发送消息datatopic " + this.dataTopic);
                            producerSend(this.dataTopic, "DBUS", data_message);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("[MaasAppenderEvent] topic: " + topic + " ,value:" + value, e);
                }
            }
        } catch (Exception e) {
            LOG.error("[MaasAppenderEvent] topic: " + topic + " ,value:" + value, e);
        } finally {
            if (dataConsumer != null) {
                dataConsumer.commitSync();
                dataConsumer.close();
                dataConsumer = null;
            }
            if (statProducer != null) {
                statProducer.flush();
                statProducer.close();
                statProducer = null;
            }
            LOG.info("[Global-Control-kafka-Consumer-event] stop. topic: " + topic + ",t:" + Thread.currentThread().getName());
        }
    }

    public boolean producerSend(String topic, final String key, String msg) {
        boolean isOk = true;
        try {
            statProducer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        LOG.error("[kafka-send-dataTopic-appender-error]", e);
                    else {
                        LOG.info("stat信息发送到topic:{}, key:{}, offset:{}",
                                new Object[]{metadata.topic(), key, metadata.offset()});
                    }
                }
            });
        } catch (Exception e) {
            isOk = false;
            LoggerFactory.getLogger().error("[kafka-send-dataTopic-error]", e);
        }
        return isOk;
    }

    public void setMessageAlter(MaasMessage data_topic_message, MaasAppenderMessage maasAppenderMessage, List<Column> columnList, String tableComment) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setTable_name(maasAppenderMessage.getTableName());
        schemaInfo.setTable_comment(tableComment);
        schemaInfo.setColumns(columnList);
        data_topic_message.setSchema_info(schemaInfo);
        data_topic_message.setDdl_sql(maasAppenderMessage.getDdl());
        data_topic_message.setMessage_type("incr");
    }

    public void setMessageColumn(MaasMessage data_topic_message, MaasAppenderMessage maasAppenderMessage, Column column) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setTable_name(maasAppenderMessage.getTableName());
        schemaInfo.setTable_comment(maasAppenderMessage.getTableComment());
        List<Column> columnList = new ArrayList<>();
        columnList.add(column);
        schemaInfo.setColumns(columnList);
        data_topic_message.setSchema_info(schemaInfo);
        data_topic_message.setDdl_sql(maasAppenderMessage.getDdl());
        data_topic_message.setMessage_type("incr");
    }

    public void setMessageTable(MaasMessage data_topic_message, MaasAppenderMessage maasAppenderMessage) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setTable_name(maasAppenderMessage.getTableName());
        schemaInfo.setTable_comment(maasAppenderMessage.getTableComment());
        List<Column> columnList = new ArrayList<>();
        schemaInfo.setColumns(columnList);
        data_topic_message.setSchema_info(schemaInfo);
        data_topic_message.setDdl_sql(maasAppenderMessage.getDdl());
        data_topic_message.setMessage_type("incr");
    }
}
