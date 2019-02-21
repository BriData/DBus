/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.util.DBUtils;
import com.creditease.dbus.util.DateUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;

/**
 * Created by Administrator on 2018/8/5.
 */
@Service
public class FlowLineCheckService {

    private static Logger logger = LoggerFactory.getLogger(FlowLineCheckService.class);

    @Autowired
    private DataTableMapper mapper;

    @Autowired
    private IZkService zkService;

    public Map<String, Boolean> check(Integer tableId) {

        Map<String, Boolean> retMap = new HashMap<>();
        KafkaConsumer<String, byte[]> consumerSecond = null;
        KafkaConsumer<String, byte[]> consumerThird = null;
        KafkaConsumer<String, byte[]> consumerFourth = null;

        try {
            retMap.put("status", true);
            Map<String, Object> infoMap = loadCheckFlowLineInfo(tableId);
            logger.info("flow line check info: {}", JSON.toJSONString(infoMap));

            List<Object> listSecond = initConsumer((String) infoMap.get("topic"), "second");
            consumerSecond = (KafkaConsumer<String, byte[]>) listSecond.get(0);
            long offsetSecond = (Long) listSecond.get(1);

            List<Object> listThird = initConsumer((String) infoMap.get("src_topic"), "third");
//            List<Object> listThird = initConsumer((String) infoMap.get("topic") + ".dbus", "third");
            consumerThird = (KafkaConsumer<String, byte[]>) listThird.get(0);
            long offsetThird = (Long) listThird.get(1);

            List<Object> listFourth = initConsumer((String) infoMap.get("output_topic"), "fourth");
            consumerFourth = (KafkaConsumer<String, byte[]>) listFourth.get(0);

            long time = System.currentTimeMillis();
            boolean isOk = firstStep(infoMap, time, retMap);
            retMap.put("firstStep", isOk);
            if (isOk) {
                isOk = secondStep(consumerSecond, time, retMap, offsetSecond);
                retMap.put("secondStep", isOk);
                if (isOk) {
                    isOk = thirdStep(consumerThird, time, retMap, offsetThird);
                    retMap.put("thirdStep", isOk);
                    if (isOk) {
                        isOk = fourthStep(consumerFourth, time, retMap);
                        retMap.put("fourthStep", isOk);
                    }
                }
            }
        } catch (Exception e) {
            retMap.put("status", false);
            logger.error(MessageFormat.format("auto check table:{0} error.", tableId), e);
        } finally {
            if (consumerSecond != null) consumerSecond.close();
            if (consumerThird != null) consumerThird.close();
            if (consumerFourth != null) consumerFourth.close();
        }
        return retMap;
    }

    private Map<String, Object> loadCheckFlowLineInfo(Integer tableId) {
        return mapper.getFlowLineCheckInfo(tableId);
    }

    private String getSendPacketSql2Oracle() {
        StringBuilder sql = new StringBuilder();
        sql.append(" insert into ");
        sql.append("     db_heartbeat_monitor (ID, DS_NAME, SCHEMA_NAME, TABLE_NAME, PACKET, CREATE_TIME)");
        sql.append(" values (SEQ_HEARTBEAT_MONITOR.NEXTVAL, ?, ?, ?, ?, to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff6'))");
        return sql.toString();
    }

    private String getSendPacketSql2Mysql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" insert into ");
        sql.append("     db_heartbeat_monitor (DS_NAME, SCHEMA_NAME, TABLE_NAME, PACKET, CREATE_TIME, UPDATE_TIME)");
        sql.append(" values (?, ?, ?, ?, ?, ?)");
        return sql.toString();
    }

    private String getSql(String dsType) {
        String sql = StringUtils.EMPTY;
        if (StringUtils.equalsIgnoreCase("mysql", dsType)) {
            sql = getSendPacketSql2Mysql();
        } else if (StringUtils.equalsIgnoreCase("oracle", dsType)) {
            sql = getSendPacketSql2Oracle();
        }
        return sql;
    }

    private boolean firstStep(Map<String, Object> infoMap, long time, Map<String, Boolean> retMap) throws Exception {
        boolean isOk = true;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dsName = (String) infoMap.get("ds_name");
            String schemaName = (String) infoMap.get("schema_name");
            String tableName = (String) infoMap.get("table_name");
            String dsPartition = (String) infoMap.get("ds_partition");

            String node = StringUtils.joinWith("/",
                    "/DBus/HeartBeat/Monitor",
                    dsName, schemaName, tableName, dsPartition);
            Map<String, Object> packet = new HashMap<>();
            packet.put("node", node);
            packet.put("time", time);
            packet.put("type", "checkpoint");
            packet.put("txTime", time);

            String dsType = (String) infoMap.get("ds_type");
            String url = (String) infoMap.get("master_url");
            String schema = (String) infoMap.get("dbus_user");
            String password = (String) infoMap.get("dbus_pwd");
            conn = DBUtils.getConn(url, schema, password);
            ps = conn.prepareStatement(getSql(dsType));
            int idx = 1;
            if (StringUtils.equalsIgnoreCase("mysql", dsType)) {
                ps.setString(idx++, dsName);
                ps.setString(idx++, schemaName);
                ps.setString(idx++, tableName);
                ps.setString(idx++, JSON.toJSONString(packet));
                ps.setString(idx++, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
                ps.setString(idx++, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            } else if (StringUtils.equalsIgnoreCase("oracle", dsType)) {
                ps.setString(idx++, dsName);
                ps.setString(idx++, schemaName);
                ps.setString(idx++, tableName);
                ps.setString(idx++, JSON.toJSONString(packet));
            }
            int cnt = ps.executeUpdate();
            if (cnt != 1) {
                isOk = false;
            }
        } catch (Exception e) {
            isOk = false;
            retMap.put("status", false);
            logger.error("auto check table first step error.", e);
        } finally {
            DBUtils.close(rs);
            DBUtils.close(ps);
            DBUtils.close(conn);
        }
        return isOk;
    }

    private void assignTopics(List<PartitionInfo> topicInfo, List<TopicPartition> assignTopics) {
        for (PartitionInfo pif : topicInfo) {
            TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
            assignTopics.add(tp);
        }
    }

    private List<Object> initConsumer(String topic, String step) throws Exception {
        Properties props = obtainKafkaConsumerProps();
        props.put("group.id", "auto-check-table-consumer-groupid-ss-" + step);
        props.put("client.id", "auto-check-table-consumer-clientid-ss-" + step);
        props.put("enable.auto.commit", false);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> assignTopics = new ArrayList<>();
        assignTopics(consumer.partitionsFor(topic), assignTopics);
        consumer.assign(assignTopics);
        consumer.seekToEnd(assignTopics);
        long position = consumer.position(assignTopics.get(0));

        logger.info("topic: {}, end position: {}", topic, position);

        List<Object> list = new ArrayList<>();
        list.add(consumer);
        list.add(position);
        return list;
    }

    private boolean secondStep(KafkaConsumer<String, byte[]> consumerSecond, long time, Map<String, Boolean> retMap, long offset) {
        boolean isOk = false;

        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumerSecond.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.offset() >= offset) {
                        isOk = true;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            retMap.put("status", false);
            logger.error("auto check table second step error.", e);
        }
        return isOk;
    }

    private boolean thirdStep(KafkaConsumer<String, byte[]> consumerThird, long time, Map<String, Boolean> retMap, long offset) {
        boolean isOk = false;
        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumerThird.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    //if (StringUtils.contains(record.key(), String.valueOf(time))) {
                    if (record.offset() >= offset) {
                        isOk = true;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            retMap.put("status", false);
            logger.error("auto check table third step error.", e);
        }
        return isOk;
    }

    private boolean fourthStep(KafkaConsumer<String, byte[]> consumerFourth, long time, Map<String, Boolean> retMap) {
        boolean isOk = false;
        try {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < 1000 * 15) && !isOk) {
                ConsumerRecords<String, byte[]> records = consumerFourth.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (StringUtils.contains(record.key(), String.valueOf(time))) {
                        isOk = true;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            retMap.put("status", false);
            logger.error("auto check table fourth step error.", e);
        }
        return isOk;
    }

    private Properties obtainKafkaConsumerProps() throws Exception {
        Properties props = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        props.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
        return props;
    }

}
