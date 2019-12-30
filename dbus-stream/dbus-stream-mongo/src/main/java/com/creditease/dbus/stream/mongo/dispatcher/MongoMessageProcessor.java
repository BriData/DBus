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


package com.creditease.dbus.stream.mongo.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.HeartBeatPacket;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import com.creditease.dbus.stream.common.tools.MessageProcessor;
import com.creditease.dbus.stream.common.tools.TableStatMap;

import java.io.IOException;
import java.util.*;

/**
 * Created by ximeiwang on 2017/12/7.
 */
public class MongoMessageProcessor extends MessageProcessor {

    public MongoMessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties producerProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {
        super(dsInfo, statTopic, producerProps, statMap, schemaTopicProps);

    }

    @Override
    public List<IGenericMessage> unwrapMessages(byte[] data) throws IOException {
        if (data == null) {
            return null;
        }
        List<IGenericMessage> list = new LinkedList<>();
        String entry = new String(data);

        MongoGenericMessage message = new MongoGenericMessage(entry);
        list.add(message);
        return list;
    }

    @Override
    public byte[] wrapMessages(List<IGenericMessage> list) throws IOException {

        //byte[] buffer = new byte[0];
        String entry = null;
        for (IGenericMessage obj : list) {
            MongoGenericMessage message = (MongoGenericMessage) obj;
            entry = message.getEntry();
        }
        //return new byte[0];
        return entry.getBytes();
    }

    @Override
    public int processMetaSyncMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        throw new RuntimeException("Impossible to here");
        //return 0;
    }

    @Override
    public int processFullPullerMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        MongoGenericMessage msg = (MongoGenericMessage) obj;
        String entry = msg.getEntry();
        JSONObject entryJson = JSON.parseObject(entry);

        String operation = entryJson.get("_op").toString();

        if (!operation.toLowerCase().equals("i")) {
            //skip it.
            logger.info("Skipped a FULL_PULL_REQUESTS message which is not INSERT Type! :" + operation);
            return -1;
        }

        //为了向下兼容，使用PULL_REMARK 保存dsName
        String dsName = null;
        String schema = null;
        String table = null;

        JSONObject document = JSON.parseObject(entryJson.getString("_o"));
        dsName = document.getString("pull_remark");
        schema = document.getString("schema_name");
        table = document.getString("table_name");

        if (dsName == null || schema == null || table == null) {
            throw new RuntimeException("解压FULL_PULL_REQUESTS 发现 dsName 或 schema 或 table为空.");
        }

        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped other datasource FULL_PULL_REQUESTS! : {}.{}.{}", dsName, schema, table);
            return -1;
        }

        logger.info(String.format("Get FULL_PULL_REQUESTS message : %s.%s.%s", dsName, schema, table));

        //单独发送一条 拉全量的消息
        List<IGenericMessage> subList = new ArrayList<>();
        subList.add(msg);
        map.put(schema, subList);

        return 0;
    }

    @Override
    public int processHeartBeatMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        MongoGenericMessage msg = (MongoGenericMessage) obj;
        String entry = msg.getEntry();
        JSONObject entryJson = JSON.parseObject(entry);

        String operation = entryJson.get("_op").toString();

        if (!operation.toLowerCase().equals("i")) {
            //skip it.
            logger.info("Skipped a DB_HEARTBEAT_MONITOR message which is not INSERT Type! :" + operation);
            return -1;
        }

        String dsName = null;
        String schemaName = null;
        String tableName = null;
        String packetJson = null;
        JSONObject document = JSON.parseObject(entryJson.getString("_o"));
        dsName = document.getString("DS_NAME");
        schemaName = document.getString("SCHEMA_NAME");
        tableName = document.getString("TABLE_NAME");
        packetJson = document.getString("PACKET");

        if (dsName == null || schemaName == null || tableName == null || packetJson == null) {
            throw new RuntimeException("DB_HEARTBEAT_MONITOR 发现 dsName 或 schema 或 table, 或 packetJson 为空.");
        }

        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped other datasource HeartBeat! : {}.{}.{}", dsName, schemaName, tableName);
            return -1;
        }

        if (packetJson.indexOf("checkpoint") >= 0) {
            HeartBeatPacket packet = HeartBeatPacket.parse(packetJson);
            statMeter(schemaName, tableName, packet.getTime(), packet.getTxtime());
        }

        List<IGenericMessage> subList = map.get(schemaName);
        if (subList != null) {
            subList.add(msg);
        } else {
            subList = new ArrayList<>();
            subList.add(msg);
            map.put(schemaName, subList);
        }
        return 0;
    }
}
