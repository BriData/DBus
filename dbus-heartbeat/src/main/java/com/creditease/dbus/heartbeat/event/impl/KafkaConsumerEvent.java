/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.KafkaConsumerContainer;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.PacketVo;

public class KafkaConsumerEvent extends AbstractEvent {

    protected String topic;

    protected Consumer<String, String> dataConsumer = null;

    protected Producer<String, String> statProducer = null;

    private ConcurrentHashMap<String, PacketVo> zkInfoCache = new ConcurrentHashMap<String, PacketVo>();

    protected TopicPartition partition0 = null;

    private Long startTime;

    private void updateZkInfoCache(String key, PacketVo packet) {
        zkInfoCache.put(key, packet);
    }

    private boolean isCanUpdateZk(Long curTime) {
        Long interval = HeartBeatConfigContainer.getInstance().getHbConf().getHeartbeatInterval();
        if ((curTime - startTime) / 1000 >= interval) {
            startTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    private void updateInfoToZk() {
        for (Map.Entry<String, PacketVo> e : zkInfoCache.entrySet()) {
            saveZk(e.getKey(), JsonUtil.toJson(e.getValue()));
            LoggerFactory.getLogger().info("[kafka-dataConsumer-event] save zk info, key:{}, time:{}", e.getKey(), e.getValue().getTime());
        }
        zkInfoCache.clear();
    }

    public KafkaConsumerEvent(String topic) {
        super(0l);
        this.topic = topic;
        Properties props = HeartBeatConfigContainer.getInstance().getKafkaConsumerConfig();
        Properties producerProps = HeartBeatConfigContainer.getInstance().getKafkaProducerConfig();
        try {
            dataConsumer = new KafkaConsumer<>(props);
            partition0 = new TopicPartition(this.topic, 0);
            dataConsumer.assign(Arrays.asList(partition0));
            dataConsumer.seekToEnd(Arrays.asList(partition0));
            KafkaConsumerContainer.getInstances().putConsumer(this.topic, dataConsumer);

            statProducer = new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        startTime = System.currentTimeMillis();
    }

    @Override
    public void run() {
        String key = "";
        try {
            while (isRun.get()) {
                try {
                    //跳过拉正在全量的topic
                    if (!StringUtils.isEmpty(EventContainer.getInstances().getSkipTargetTopic(this.topic))) {
                        dataConsumer.seekToEnd(Arrays.asList(partition0));
                        Thread.sleep(1000);
                        continue;
                    }

                    //不管是否读到数据，到时间就更新zk
                    if (isCanUpdateZk(System.currentTimeMillis())) {
                        updateInfoToZk();
                    }

                    ConsumerRecords<String, String> records = dataConsumer.poll(1000);
                    if (records.isEmpty()) {
                        continue;
                    }

                    long cpTime = -1;
                    long txTime = -1;

                    for (ConsumerRecord<String, String> record : records) {
                        key = record.key();

                        if (StringUtils.isEmpty(key)) {
                            LoggerFactory.getLogger().error("[kafka-dataConsumer-event]  topic:{}, key:null, offset:{}", topic, record.offset());
                            continue;
                        }

                        long curTime = System.currentTimeMillis();
                        //旧的格式 总共 有 4 个字段:
                        // 1 普通数据  datasource.schema1.table1.time
                        //    例子：      cedb.APPENDER.T11.1473493041856
                        // 2 心跳格式  有 5 个字段：
                        //  1) 带2个时间的 datasource.schema1.table1.time|txTime.heartbeat
                        //    例子:       cedb.APPENDER.T11.1473493041856|1473493041846.heartbeat

                        //新的格式有 10个字段
                        //      普通 data_increment_data.mysql.db1.schema1.table1.5.0.0.time.wh
                        //      例子 data_increment_data.mysql.db1.schema1.table1.5.0.0.1481245701166.wh
                        //      心跳 data_increment_heartbeat.mysql.db1.schema1.table1.5.0.0.time|txTime|ok.wh
                        //      例子 data_increment_heartbeat.mysql.db1.schema1.table1.5.0.0.1481245701166|1481245700947|ok.wh
                        String[] vals = StringUtils.split(key, ".");
                        if (vals == null) {
                            LoggerFactory.getLogger().error("[kafka-dataConsumer-event] receive heartbeart topic:{}, key:{}", topic, key);
                            continue;
                        }

                        String dsName = null;
                        String schemaName = null;
                        String tableName = null;

                        boolean isTableOK = true;

                        if (vals.length == 4) {
                            //旧版有数据来, table正常
                            dsName = vals[0];
                            schemaName = vals[1];
                            tableName = vals[2];
                            // time
                            cpTime = Long.valueOf(vals[3]);
                        } else if (vals.length == 5) {
                            //旧版
                            dsName = vals[0];
                            schemaName = vals[1];
                            tableName = vals[2];
                            if (StringUtils.contains(vals[3], "|")) {
                                //  带2个时间的 datasource.schema1.table1.time|txTime.heartbeat
                                String times[] = StringUtils.split(vals[3], "|");
                                cpTime = Long.valueOf(times[0]);
                                txTime = Long.valueOf(times[1]);
                                //有心跳来, 但无法判断 是否数据被abort了,因为没有状态
                                sendStatMsg(dsName, schemaName, tableName, cpTime, txTime, curTime, key);
                            } else {
                                LOG.error("it should not be here. key:{}", key);
                            }
                        } else if (vals.length == 10) {
                            dsName = vals[2];
                            schemaName = vals[3];
                            tableName = vals[4];
                            if (vals[0].equals("data_increment_data")) {
                                //有数据来, table正常的情况
                                // ojjTime
                                //cpTime = Long.valueOf(vals[8]);
                                isTableOK = false;
                            } else if (vals[0].equals("data_increment_heartbeat")) {
                                //新版  time|txTime|status
                                if (StringUtils.contains(vals[8], "|")) {
                                    String times[] = StringUtils.split(vals[8], "|");
                                    cpTime = Long.valueOf(times[0]);
                                    txTime = Long.valueOf(times[1]);
                                    if (times.length == 3 && times[2].equals("abort")) {
                                        //表明 其实表已经abort了，但心跳数据仍然
                                        //这种情况，只发送stat，不更新zk
                                        isTableOK = false;
                                    }
                                    sendStatMsg(dsName, schemaName, tableName, cpTime, txTime, curTime, key);
                                } else {
                                    LOG.error("it should not be here. key:{}", key);
                                }
                            }
                        } else {
                            LoggerFactory.getLogger().error("[kafka-dataConsumer-event] receive heartbeart topic:{}, key:{}", topic, key);
                            continue;
                        }

                        if (isTableOK) {
                            //更新zk表状态时间

                            String path = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                            path = StringUtils.join(new String[]{path, dsName, schemaName, tableName}, "/");
                            // 反序列化packet信息
                            PacketVo packet = deserialize(path, PacketVo.class);
                            if (packet == null) {
                                continue;
                            }

                            //积压的msg时也报警，因此读kafka时，读当前时间
                            packet.setTime(cpTime);

                            packet.setTxTime(txTime);

                            //更新cache，避免狂刷 zk
                            updateZkInfoCache(path, packet);
                        }

                        //不管是否读到数据，到时间就更新zk
                        if (isCanUpdateZk(System.currentTimeMillis())) {
                            updateInfoToZk();
                        }
                    }


                } catch (Exception e) {
                    LoggerFactory.getLogger().error("[kafka-dataConsumer-event] topic: " + topic + " ,key:" + key, e);
                    //stop();
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[kafka-dataConsumer-event] topic: " + topic + " ,key:" + key, e);
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
        }
        LoggerFactory.getLogger().info("[kafka-dataConsumer-event] stop. topic: " + topic + ",t:" + Thread.currentThread().getName());
    }

    private void sendStatMsg(String dsName, String schemaName, String tableName, long cpTime, long txTime, long curTime, String key) {
        //这个是带有checkpoint的心跳包
        StatMessage sm = new StatMessage(dsName, schemaName, tableName, "HEART_BEAT");
        sm.setCheckpointMS(cpTime);
        sm.setTxTimeMS(txTime);
        sm.setLocalMS(curTime);
        sm.setLatencyMS(curTime - cpTime);
        producerSend("dbus_statistic", key, sm.toJSONString());
    }

    public boolean producerSend(String topic, final String key, String msg) {
        boolean isOk = true;
        try {
            statProducer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {

                    if (e != null)
                        LOG.error("[kafka-send-error]", e);
                    else {
                        LOG.info("stat信息发送到topic:{}, key:{}, offset:{}",
                                new Object[]{metadata.topic(), key, metadata.offset()});
                    }
                }
            });
        } catch (Exception e) {
            isOk = false;
            LoggerFactory.getLogger().error("[kafka-send-error]", e);
        }
        return isOk;
    }
}
