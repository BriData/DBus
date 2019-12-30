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


package com.creditease.dbus.allinone.auto.check.handler.impl;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.utils.DBUtils;
import com.creditease.dbus.allinone.auto.check.utils.DateUtil;
import com.creditease.dbus.allinone.auto.check.utils.MsgUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by Administrator on 2018/8/1.
 */
public class CheckFlowLineHandler extends AbstractHandler {


    @Override
    public void check(BufferedWriter bw) throws Exception {

        KafkaConsumer<String, byte[]> consumerSecond = null;
        KafkaConsumer<String, byte[]> consumerThird = null;
        KafkaConsumer<String, byte[]> consumerFourth = null;

        try {
            bw.newLine();
            bw.write("check flow line start: ");
            bw.newLine();
            bw.write("============================================");
            bw.newLine();

            List<Object> listSecond = initConsumer("testdb", "second");
            consumerSecond = (KafkaConsumer<String, byte[]>) listSecond.get(0);
            long offsetSecond = (Long) listSecond.get(1);

            List<Object> listThird = initConsumer("testdb.testschema", "third");
            consumerThird = (KafkaConsumer<String, byte[]>) listThird.get(0);
            long offsetThird = (Long) listThird.get(1);

            List<Object> listFourth = initConsumer("testdb.testschema.result", "fourth");
            consumerFourth = (KafkaConsumer<String, byte[]>) listFourth.get(0);
            // long offsetFourth = (Long) listFourth.get(1);

            long time = System.currentTimeMillis();

            firstStep(bw, time);
            secondStep(bw, consumerSecond, offsetSecond);
            thirdStep(bw, consumerThird, offsetThird);
            fourthStep(bw, consumerFourth, time);
        } catch (Exception e) {
            throw e;
        } finally {
            if (consumerSecond != null) consumerSecond.close();
            if (consumerThird != null) consumerThird.close();
            if (consumerFourth != null) consumerFourth.close();
        }

    }

    /**
     * 插入心跳
     *
     * @param bw
     * @throws Exception
     */
    private void firstStep(BufferedWriter bw, long time) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(" insert into ");
        sql.append("     db_heartbeat_monitor (DS_NAME, SCHEMA_NAME, TABLE_NAME, PACKET, CREATE_TIME, UPDATE_TIME)");
        sql.append(" values (?, ?, ?, ?, ?, ?)");

        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String url = MsgUtil.format("jdbc:mysql://{0}:{1}/{2}?characterEncoding=utf-8",
                    conf.getDbDbusHost(), String.valueOf(conf.getDbDbusPort()), "dbus");

            Map<String, Object> packet = new HashMap<>();
            packet.put("node", "/DBus/HeartBeat/Monitor/testdb/testschema/test_table/0");
            packet.put("time", time);
            packet.put("type", "checkpoint");
            packet.put("txTime", time);

            conn = DBUtils.getConn(url, conf.getDbDbusSchema(), conf.getDbDbusPassword());
            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, "testdb");
            ps.setString(2, "testschema");
            ps.setString(3, "test_table ");
            ps.setString(4, JSON.toJSONString(packet));
            ps.setString(5, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            ps.setString(6, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));

            int cnt = ps.executeUpdate();
            if (cnt == 1) {
                bw.write("first step insert heart beat success.");
                bw.newLine();
            } else {
                bw.write("first step insert heart beat fail.");
                bw.newLine();
            }
        } catch (Exception e) {
            bw.write("first step insert heart beat error: " + e.getMessage());
            bw.newLine();
            throw new RuntimeException("first step insert heart beat error", e);
        } finally {
            DBUtils.close(rs);
            DBUtils.close(ps);
            DBUtils.close(conn);
        }

    }

    private void assignTopics(List<PartitionInfo> topicInfo, List<TopicPartition> assignTopics) {
        for (PartitionInfo pif : topicInfo) {
            TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
            assignTopics.add(tp);
        }
    }

    private List<Object> initConsumer(String topic, String step) {
        Properties props = obtainKafkaConsumerProps();
        props.put("group.id", "auto-check-allinone-consumer-groupid-ss-" + step);
        props.put("client.id", "auto-check-allinone-consumer-clientid-ss-" + step);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> assignTopics = new ArrayList<>();
        assignTopics(consumer.partitionsFor(topic), assignTopics);
        consumer.assign(assignTopics);
        consumer.seekToEnd(assignTopics);
        long position = consumer.position(assignTopics.get(0));

        List<Object> list = new ArrayList<>();
        list.add(consumer);
        list.add(position);
        return list;
    }

    private void secondStep(BufferedWriter bw, KafkaConsumer<String, byte[]> consumer, long offset) throws Exception {
        boolean isOk = false;
        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.offset() >= offset) {
                        isOk = true;
                        bw.write("data arrive at topic: " + record.topic());
                        bw.newLine();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            bw.write("auto check table second step error: " + e.getMessage());
            bw.newLine();
            throw new RuntimeException("auto check table second step error", e);
        }

        if (!isOk) {
            bw.write("flow line second step time out");
            throw new RuntimeException("flow line second step time out");
        }
    }

    private void thirdStep(BufferedWriter bw, KafkaConsumer<String, byte[]> consumer, long offset) throws Exception {
        boolean isOk = false;
        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.offset() >= offset) {
                        isOk = true;
                        bw.write("data arrive at topic: " + record.topic());
                        bw.newLine();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            bw.write("auto check table third step error: " + e.getMessage());
            bw.newLine();
            throw new RuntimeException("auto check table third step error", e);
        }

        if (!isOk) {
            bw.write("flow line third step time out");
            bw.newLine();
            throw new RuntimeException("flow line third step time out");
        }
    }

    private void fourthStep(BufferedWriter bw, KafkaConsumer<String, byte[]> consumer, long time) throws Exception {
        boolean isOk = false;
        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumer.poll(0);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (StringUtils.contains(record.key(), String.valueOf(time))) {
                        isOk = true;
                        bw.write("data arrive at topic: " + record.topic());
                        bw.newLine();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            bw.write("auto check table fourth step error: " + e.getMessage());
            bw.newLine();
            throw new RuntimeException("auto check table fourth step error", e);
        }

        if (!isOk) {
            bw.write("flow line fourth step time out");
            bw.newLine();
            throw new RuntimeException("flow line fourth step time out");
        }

    }

    private Properties obtainKafkaConsumerProps() {
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getKafkaBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("enable.auto.commit", false);
        props.put("auto.commit.interval.ms", 1000);
        props.put("max.partition.fetch.bytes", 10485760);
        props.put("max.poll.records", 30);
        props.put("session.timeout.ms", 30000);
        return props;
    }
}
