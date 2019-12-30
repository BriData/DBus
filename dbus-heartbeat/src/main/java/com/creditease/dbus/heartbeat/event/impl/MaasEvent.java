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

import com.alibaba.fastjson.JSONObject;
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by dashencui on 2017/9/12.
 */
public class MaasEvent extends AbstractEvent {

    private int waitingTimes = 0;

    private IDbusDataDao dao;

    protected String topic;

    protected String dataTopic;

    protected Consumer<String, String> dataConsumer = null;

    protected Producer<String, String> statProducer = null;

    protected TopicPartition partition0 = null;


    public MaasEvent(String topic, String dataTopic) {
        super(01);
        this.topic = topic;
        this.dataTopic = dataTopic;
        dao = new DbusDataDaoImpl();
        Properties props = HeartBeatConfigContainer.getInstance().getmaasConf().getConsumerProp();
        Properties producerProps = HeartBeatConfigContainer.getInstance().getmaasConf().getProducerProp();
        try {
            LoggerFactory.getLogger().info("[topic]   ...." + topic);
            LoggerFactory.getLogger().info("[maas-event]  initial.........................");
            dataConsumer = new KafkaConsumer<>(props);
            partition0 = new TopicPartition(this.topic, 0);
            dataConsumer.assign(Arrays.asList(partition0));
            dataConsumer.seekToEnd(Arrays.asList(partition0));

            statProducer = new KafkaProducer<>(producerProps);

        } catch (Exception e) {
            e.printStackTrace();
            LoggerFactory.getLogger().error(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        String key = "";
        String value = "";
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
                            LoggerFactory.getLogger().info("[maas-event]  polling ....");
                        }
                        continue;
                    }
                    waitingTimes = 0;

                    LoggerFactory.getLogger().info("[maas-event]  polled data size: {}", records.count());
                    LoggerFactory.getLogger().debug("[maas-event]  polled data size: {}", records);

                    for (ConsumerRecord<String, String> record : records) {
                        key = record.key();
                        value = record.value();

                        Map<String, String> maas_map = value2map(value);
                        String host = maas_map.get("host");
                        String port = maas_map.get("port");
                        String instance_name = maas_map.get("instance_name");
                        String ds_name = maas_map.get("ds_name");
                        String schemaName = maas_map.get("schemaName");
                        String tableName = maas_map.get("tableName");
                        //根据读到的数据确定表
                        int count_ds_id = dao.queryCountDsId(Constants.CONFIG_DB_KEY, host, port, instance_name);
                        LoggerFactory.getLogger().info("[maas-event]  ds id count: {}", count_ds_id);
                        List<Long> ver_id_list = new ArrayList<>();
                        if (count_ds_id == 1) {
                            ver_id_list = dao.queryDsFromDbusData(Constants.CONFIG_DB_KEY, host, port, instance_name, schemaName, tableName);
                        } else {
                            ver_id_list = dao.queryDsFromDbusData_use_dsname(Constants.CONFIG_DB_KEY, ds_name, schemaName, tableName);
                        }

                        LoggerFactory.getLogger().info("[maas-event]  ver id list {}", ver_id_list);

                        if (ver_id_list.isEmpty()) {
                            LoggerFactory.getLogger().info("[maas-event] ", "没有找到数据源");
                            producerSend(this.dataTopic, "DBUS", "没有找到数据");
                            continue;
                        }
                        if (!tableName.isEmpty()) {
                            /*有指定表的情况，发一条消息到指定kafka*/

                            long ver_id = ver_id_list.get(0);
                            MaasMessage data_topic_message = new MaasMessage();
                            DataSource dataSource = dao.queryDsInfoFromDbusData(Constants.CONFIG_DB_KEY, ver_id);//获得数据源信息
                            String table_comment = dao.queryTableComment(Constants.CONFIG_DB_KEY, ver_id, schemaName);//获取表解释
                            List<Column> column_list = dao.queryColumsUseVerid(Constants.CONFIG_DB_KEY, ver_id);//获得所有列信息

                            //构造要发送的消息
                            data_topic_message.setData_source(dataSource);
                            data_topic_message.setObject_owner(schemaName);
                            SchemaInfo schemaInfo = new SchemaInfo();
                            schemaInfo.setTable_name(tableName);
                            schemaInfo.setTable_comment(table_comment);
                            schemaInfo.setColumns(column_list);
                            data_topic_message.setSchema_info(schemaInfo);
                            data_topic_message.setDdl_sql("");
                            data_topic_message.setMessage_type("init");

                            //发送消息
                            String data_message = data_topic_message.toString();
                            LoggerFactory.getLogger().info("发送的消息 " + data_message);
                            producerSend(this.dataTopic, "DBUS", data_message);
                        } else {
                            for (Long ver_id : ver_id_list) {
                                MaasMessage data_topic_message = new MaasMessage();
                                DataSource dataSource = dao.queryDsInfoFromDbusData(Constants.CONFIG_DB_KEY, ver_id);//获得数据源信息
                                tableName = dao.queryTableName(Constants.CONFIG_DB_KEY, ver_id, schemaName);
                                String table_comment = dao.queryTableComment(Constants.CONFIG_DB_KEY, ver_id, schemaName);//获取表解释
                                List<Column> column_list = dao.queryColumsUseVerid(Constants.CONFIG_DB_KEY, ver_id);//获得所有列信息

                                //构造要发送的消息
                                data_topic_message.setData_source(dataSource);
                                data_topic_message.setObject_owner(schemaName);
                                SchemaInfo schemaInfo = new SchemaInfo();
                                schemaInfo.setTable_name(tableName);
                                schemaInfo.setTable_comment(table_comment);
                                schemaInfo.setColumns(column_list);
                                data_topic_message.setSchema_info(schemaInfo);
                                data_topic_message.setDdl_sql("");
                                data_topic_message.setMessage_type("init");

                                //发送消息
                                String data_message = data_topic_message.toString();
                                LoggerFactory.getLogger().info("发送的消息 " + data_message);
                                producerSend(this.dataTopic, "DBUS", data_message);
                            }

                        }

                    }
                } catch (Exception e) {
                    LOG.error("[[maas-event]] topic: " + topic + " ,key:" + key, e);
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[maas-event] topic: " + topic + " ,key:" + key, e);
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
            LOG.info("dataTopic" + topic);
            statProducer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        LOG.error("[kafka-send-dataTopic-error]", e);
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

    public Map<String, String> value2map(String value) {
        JSONObject jsonValue = JSONObject.parseObject(value);
        LoggerFactory.getLogger().info("[maas-event] jsonValue" + jsonValue.toJSONString());
        JSONObject ds = jsonValue.getJSONObject("data_source");
        LoggerFactory.getLogger().info("[maas-event] data_source" + ds.toJSONString());
        JSONObject server = ds.getJSONObject("Server");
        String host = server.getString("host");
        String port = server.getString("port");
        String instance_name = ds.getString("instance_name");
        String ds_name = ds.getString("ds_name");
        String schemaName = jsonValue.getString("owner");
        String tableName = jsonValue.getString("object_name");
        Map<String, String> maas_map = new HashMap<String, String>();
        maas_map.put("host", host);
        maas_map.put("port", port);
        maas_map.put("instance_name", instance_name);
        maas_map.put("ds_name", ds_name);
        maas_map.put("schemaName", schemaName);
        maas_map.put("tableName", tableName);
        return maas_map;
    }

}
