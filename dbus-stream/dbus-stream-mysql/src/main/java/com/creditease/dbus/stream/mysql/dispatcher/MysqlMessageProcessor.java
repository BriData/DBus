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


package com.creditease.dbus.stream.mysql.dispatcher;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.HeartBeatPacket;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import com.creditease.dbus.stream.common.tools.MessageProcessor;
import com.creditease.dbus.stream.common.tools.TableStatMap;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by dongwang47 on 2016/8/18.
 */
public class MysqlMessageProcessor extends MessageProcessor {

    public MysqlMessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties producerProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {
        super(dsInfo, statTopic, producerProps, statMap, schemaTopicProps);

    }

    @Override
    public List<IGenericMessage> unwrapMessages(byte[] data) throws IOException {
        List<IGenericMessage> list = new ArrayList<>();

        CanalPacket.Messages cMessage = CanalPacket.Messages.parseFrom(data);
        List<ByteString> strings = cMessage.getMessagesList();
        for (ByteString str : strings) {
            CanalEntry.Entry ent = CanalEntry.Entry.parseFrom(str);
            MysqlGenericMessage message = new MysqlGenericMessage(ent);

            list.add(message);
        }
        return list;
    }

    @Override
    public byte[] wrapMessages(List<IGenericMessage> list) throws IOException {
        CanalPacket.Messages.Builder builder = CanalPacket.Messages.newBuilder();
        builder.setBatchId(0);

        for (IGenericMessage obj : list) {
            MysqlGenericMessage message = (MysqlGenericMessage) obj;
            CanalEntry.Entry ent = message.getEntry();

            builder.addMessages(ent.toByteString());
        }
        CanalPacket.Messages cMessage = builder.build();
        return cMessage.toByteArray();
    }

    @Override
    public int processMetaSyncMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        throw new RuntimeException("Impossible to here");
    }

    @Override
    public int processFullPullerMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        MysqlGenericMessage message = (MysqlGenericMessage) obj;
        CanalEntry.Entry entry = message.getEntry();

        CanalEntry.EventType eventType = entry.getHeader().getEventType();
        if (eventType != CanalEntry.EventType.INSERT) {
            //skip it.
            logger.info("Skipped a FULL_PULL_REQUESTS message which is not INSERT Type! :" + eventType.toString());
            return -1;
        }

        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        List<CanalEntry.RowData> dataList = rowChange.getRowDatasList();
        if (dataList.size() != 1) {
            throw new RuntimeException(String.format("解压FULL_PULL_REQUESTS 发现 %d 条bach数据，应该只有一条", dataList.size()));
        }

        //为了向下兼容，使用PULL_REMARK 保存dsName
        String dsName = null;
        String schema = null;
        String table = null;
        List<CanalEntry.Column> columns = dataList.get(0).getAfterColumnsList();
        for (CanalEntry.Column column : columns) {
            if (column.getName().equalsIgnoreCase("PULL_REMARK")) {
                dsName = column.getValue();
            } else if (column.getName().equalsIgnoreCase("SCHEMA_NAME")) {
                schema = column.getValue();
            } else if (column.getName().equalsIgnoreCase("TABLE_NAME")) {
                table = column.getValue();
            }
        }

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
        subList.add(message);
        map.put(schema, subList);
        return 0;
    }

    @Override
    public int processHeartBeatMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        MysqlGenericMessage message = (MysqlGenericMessage) obj;
        CanalEntry.Entry entry = message.getEntry();

        CanalEntry.EventType eventType = entry.getHeader().getEventType();
        if (eventType != CanalEntry.EventType.INSERT) {
            //skip it.
            logger.info("Skipped a DB_HEARTBEAT_MONITOR message which is not INSERT Type! :" + eventType.toString());
            return -1;
        }

        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        List<CanalEntry.RowData> dataList = rowChange.getRowDatasList();
        /**
         * 以前代码dataList.size() 必须是1条数据，发现生产中有奇怪数据，心跳表数据居然不止一个心跳数据，推测是 insert ... select ...
         * 这种情况我们不处理，打算直接将这个心跳包抛弃。
         */
//        if (dataList.size() != 1) {
//            throw new RuntimeException(String.format("DB_HEARTBEAT_MONITOR 发现 %d 条bach数据，应该只有一条", dataList.size()));
//        }
        if (dataList.size() != 1) {
            logger.error(String.format("Skipped a DB_HEARTBEAT_MONITOR message. DB_HEARTBEAT_MONITOR 发现 %d 条bach数据，应该只有一条, 语句是%s",
                    dataList.size(), rowChange.getSql()));
            return -1;
        }

        String dsName = null;
        String schemaName = null;
        String tableName = null;
        String packetJson = null;
        List<CanalEntry.Column> columns = dataList.get(0).getAfterColumnsList();
        for (CanalEntry.Column column : columns) {
            if (column.getName().equalsIgnoreCase("DS_NAME")) {
                dsName = column.getValue();
            } else if (column.getName().equalsIgnoreCase("SCHEMA_NAME")) {
                schemaName = column.getValue();
            } else if (column.getName().equalsIgnoreCase("TABLE_NAME")) {
                tableName = column.getValue();
            } else if (column.getName().equalsIgnoreCase("PACKET"))
                packetJson = column.getValue();
        }

        if (dsName == null || schemaName == null || tableName == null || packetJson == null) {
            throw new RuntimeException("DB_HEARTBEAT_MONITOR 发现 dsName 或 schema 或 table, 或 packetJson 为空.");
        }

        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped other datasource HeartBeat! : {}.{}.{}", dsName, schemaName, tableName);
            return -1;
        }

        logger.debug(String.format("Get DB_HEARTBEAT_MONITOR message : %s.%s, packetJson: %s", schemaName, tableName, packetJson));
        if (packetJson.indexOf("checkpoint") >= 0) {
            logger.debug(String.format("Get DB_HEARTBEAT_MONITOR message, prepare set stat message. "));
            HeartBeatPacket packet = HeartBeatPacket.parse(packetJson);
            statMeter(schemaName, tableName, packet.getTime(), packet.getTxtime());
        }

        List<IGenericMessage> subList = map.get(schemaName);
        if (subList != null) {
            subList.add(message);
        } else {
            subList = new ArrayList<>();
            subList.add(message);
            map.put(schemaName, subList);
        }
        return 0;
    }
}
