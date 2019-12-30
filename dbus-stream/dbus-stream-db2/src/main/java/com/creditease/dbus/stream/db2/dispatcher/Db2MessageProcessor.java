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


package com.creditease.dbus.stream.db2.dispatcher;


import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.HeartBeatPacket;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import com.creditease.dbus.stream.common.tools.MessageProcessor;
import com.creditease.dbus.stream.common.tools.TableStatMap;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhenlinzhong on 2018/4/26.
 */
public class Db2MessageProcessor extends MessageProcessor {

    private Db2GenericSchemaDecoder decoder;

    public Db2MessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties producerProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {
        super(dsInfo, statTopic, producerProps, statMap, schemaTopicProps);
        decoder = Db2GenericSchemaDecoder.getInstance(dsInfo.getSchemaRegistryRestUrl());
    }

    @Override
    public List<IGenericMessage> unwrapMessages(byte[] data) throws IOException {
//        return decoder.unwrap(data);
        return null;
    }


    @Override
    public List<IGenericMessage> unwrapDb2Messages(DBusConsumerRecord record) throws IOException {
        return decoder.unwrap(record);
    }


    @Override
    public byte[] wrapMessages(List<IGenericMessage> list) throws IOException {
        return decoder.wrap(list);
    }

    @Override
    public int processMetaSyncMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        return 0;
    }

    @Override
    public int processFullPullerMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {
        Db2GenericMessage msg = (Db2GenericMessage) obj;
        //拉全量表，只发给需要拉全量的数据
        List<GenericRecord> records = decoder.decodeFullPull(msg);
        if (records.size() != 1) {
            throw new RuntimeException(String.format("解压发现 %d 条payload，应该只有一条", records.size()));
        }

        GenericRecord fullPullMsg = records.get(0);

        String type = fullPullMsg.get(Constants.DB2MessageBodyKey.DB2_ENTTYP).toString();
        if (!type.equalsIgnoreCase(Constants.DB2MessageBodyKey.PT)) {
            //skip it.
            logger.info("Skipped a FULL_PULL_REQUESTS message which is not INSERT Type! :" + type);
            return -1;
        }

        //为了向下兼容，使用PULL_REMARK 保存dsName
        if (records.get(0).get("PULL_REMARK") == null) {
            logger.info("Skipped a FULL_PULL_REQUESTS message which PULL_REMARK is null");
            return -1;
        }

        String dsName = fullPullMsg.get("PULL_REMARK").toString();
        String schema = fullPullMsg.get("SCHEMA_NAME").toString();
        String table = fullPullMsg.get("TABLE_NAME").toString();

        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped a FULL_PULL_REQUESTS message which is not the current datasource : {}.{}.{}", dsName, schema, table);
            return -1;
        }

        logger.info(String.format("Get FULL_PULL_REQUESTS message : %s.%s.%s", dsName, schema, table));

        //单独发送一条 拉全量的消息
        List<IGenericMessage> subList = new ArrayList<>();
        subList.add(msg);
        map.put(msg.getSchemaName(), subList);
        return 0;
    }

    @Override
    public int processHeartBeatMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) {
        Db2GenericMessage msg = (Db2GenericMessage) obj;

        List<GenericRecord> records = null;

        try {
            records = decoder.decodeHeartBeat(msg);
        } catch (IOException e) {
            logger.error("解析心跳数据失败！" + e);
        }

        if (records.size() != 1) {
            throw new RuntimeException(String.format("解压发现 %d 条payload，应该只有一条", records.size()));
        }

        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertDB2AvroRecord(records.get(0));

        String type = wrapper.getPair(Constants.DB2MessageBodyKey.DB2_ENTTYP).getValue().toString();
        //logger.info("接收到心跳数据，类型： {}, 心跳包数据： {}", type, wrapper.getPairValue("PACKET").toString());
        if (!type.equalsIgnoreCase(Constants.DB2MessageBodyKey.PT)) {
            //skip it.
            logger.info("Skipped a DB_HEARTBEAT_MONITOR message which is not INSERT Type! :" + type);
            return -1;
        }

        String dsName = wrapper.getPairValue("DS_NAME").toString();
        String schemaName = wrapper.getPairValue("SCHEMA_NAME").toString();
        String tableName = wrapper.getPairValue("TABLE_NAME").toString();
        String packetJson = wrapper.getPairValue("PACKET").toString();
        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped other datasource HeartBeat! : {}.{}.{}", dsName, schemaName, tableName);
            return -1;
        }

        if (!schemaTopicProps.containsKey(schemaName)) {
            logger.warn("An unknown schema found in heartbeat table: " + schemaName);
        }

        if (packetJson.indexOf("checkpoint") >= 0) {
            HeartBeatPacket packet = HeartBeatPacket.parse(packetJson);
            statMeter(schemaName, tableName, packet.getTime(), packet.getTxtime());
        }

        List<IGenericMessage> subList = map.get(msg.getSchemaName());
        if (subList != null) {
            subList.add(msg);
        } else {
            subList = new ArrayList<>();
            subList.add(msg);
            map.put(msg.getSchemaName(), subList);
        }

        return 0;
    }
}
