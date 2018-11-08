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

package com.creditease.dbus.stream.oracle.dispatcher;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.HeartBeatPacket;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import com.creditease.dbus.stream.common.tools.MessageProcessor;
import com.creditease.dbus.stream.common.tools.TableStatMap;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.*;

/**
 */
public class OracleMessageProcessor extends MessageProcessor {

    private static Set<String> noorderKeys = Constants.MessageBodyKey.noorderKeys;

    private OracleGenericSchemaDecoder decoder;


    public OracleMessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties producerProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {
        super(dsInfo, statTopic, producerProps, statMap, schemaTopicProps);

        decoder = OracleGenericSchemaDecoder.getInstance();
    }


    @Override
    public List<IGenericMessage> unwrapMessages(byte[] data) throws IOException {
        return decoder.unwrap(data);
    }

    @Override
    public byte[] wrapMessages(List<IGenericMessage> list) throws IOException {
        return decoder.wrap (list);
    }

    @Override
    public int processMetaSyncMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {

        OracleGenericMessage msg = (OracleGenericMessage) obj;

        //Syn event，只发给相关的schema
        List<GenericRecord> records = decoder.decodeSyncEvent(msg.getSchemaHash(), msg.getPayload());
        if (records.size() != 1) {
            throw new RuntimeException(String.format("解压发现 %d 条payload，应该只有一条", records.size()));
        }

        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord (records.get(0), noorderKeys);

        String type = wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString();
        if (!type.equalsIgnoreCase("I")) {
            //skip it.
            logger.info ("Skipped a META_SYNC_EVENT message which is not INSERT Type! :" + type);
            return -1;
        }

        String schema = wrapper.getPairValue("TABLE_OWNER").toString();
        String table = wrapper.getPairValue("TABLE_NAME").toString();

        logger.info(String.format("Get META_SYNC_EVENT message: %s.%s", schema, table));

        // 单独发送一条 meta_sync的消息
        List<IGenericMessage> subList = new LinkedList<>();
        subList.add(msg);
        map.put(schema, subList);

        return 0;
    }

    @Override
    public int processFullPullerMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException {

        OracleGenericMessage msg = (OracleGenericMessage) obj;

        //拉全量表，只发给需要拉全量的数据
        List<GenericRecord> records = decoder.decodeFullPull(msg.getSchemaHash(), msg.getPayload());
        if (records.size() != 1) {
            throw new RuntimeException(String.format("解压发现 %d 条payload，应该只有一条", records.size()));
        }

        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord (records.get(0), noorderKeys);

        String type = wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString();
        if (!type.equalsIgnoreCase("I")) {
            //skip it.
            logger.info ("Skipped a FULL_PULL_REQUESTS message which is not INSERT Type! :" + type);
            return -1;
        }

        //为了向下兼容，使用PULL_REMARK 保存dsName

        if(wrapper.getPairValue("PULL_REMARK") == null) {
            logger.info("Skipped a FULL_PULL_REQUESTS message which PULL_REMARK is null");
            return -1;
        }

        String dsName = wrapper.getPairValue("PULL_REMARK").toString();
        String schema = wrapper.getPairValue("SCHEMA_NAME").toString();
        String table = wrapper.getPairValue("TABLE_NAME").toString();

        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.info("Skipped a FULL_PULL_REQUESTS message which is not the current datasource : {}.{}.{}" , dsName, schema, table);
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
        OracleGenericMessage msg = (OracleGenericMessage) obj;

        List<GenericRecord> records = decoder.decodeHeartBeat(msg.getSchemaHash(), msg.getPayload());
        if (records.size() != 1) {
            throw new RuntimeException(String.format("解压发现 %d 条payload，应该只有一条", records.size()));
        }

        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord (records.get(0), noorderKeys);

        String type = wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString();
        if (!type.equalsIgnoreCase("I")) {
            //skip it.
            logger.debug("Skipped a DB_HEARTBEAT_MONITOR message which is not INSERT Type! :" + type);
            return -1;
        }

        String dsName = wrapper.getPairValue("DS_NAME").toString();
        String schemaName = wrapper.getPairValue("SCHEMA_NAME").toString();
        String tableName = wrapper.getPairValue("TABLE_NAME").toString();
        String packetJson = wrapper.getPairValue("PACKET").toString();
        if (!dsName.equalsIgnoreCase(dsInfo.getDbSourceName())) {
            logger.debug("Skipped other datasource HeartBeat! : {}.{}.{}" , dsName, schemaName, tableName);
            return -1;
        }

        if (!schemaTopicProps.containsKey(schemaName)) {
            logger.warn("An unknown schema found in heartbeat table: " + schemaName);
        }

        if (packetJson.indexOf("checkpoint") >= 0) {

            HeartBeatPacket packet = HeartBeatPacket.parse(packetJson);
            statMeter(schemaName, tableName, packet.getTime(), packet.getTxtime());
        }


        List<IGenericMessage> subList = map.get(schemaName);
        if (subList != null) {
            subList.add(msg);
        } else  {
            subList = new ArrayList<>();
            subList.add(msg);
            map.put(schemaName, subList);
        }

        return 0;
    }
}
