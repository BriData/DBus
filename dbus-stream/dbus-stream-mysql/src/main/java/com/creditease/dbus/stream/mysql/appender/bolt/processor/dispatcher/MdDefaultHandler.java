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


package com.creditease.dbus.stream.mysql.appender.bolt.processor.dispatcher;

import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.mysql.appender.protobuf.convertor.Convertor;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MsgColumn;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * mysql dispatcher default handler
 *
 * @author xiongmao
 */
public class MdDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    private Tuple tuple;
    private EmitData emitData;

    public MdDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        this.tuple = tuple;
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        this.emitData = emitData;
        List<MessageEntry> datas = emitData.get(EmitData.GENERIC_DATA_LIST);
        Command cmd = (Command) tuple.getValueByField(Constants.EmitFields.COMMAND);
        String heartbeatNs = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.HEARTBEAT_SRC);
        groupSend(datas, heartbeatNs, cmd);
    }

    private void groupSend(List<MessageEntry> datas, String heartbeatNs, Command defaultCmd) {
        Map<String, GroupData> map = Maps.newLinkedHashMap();
        boolean lastElemIsHeartbeat = false;
        for (MessageEntry entry : datas) {
            try {
                // 判断是否为心跳表的数据,如果是心跳表的数据则按照心跳表的schema_name和table_name进行分发
                String namespace = StringUtils.join(new String[]{entry.getEntryHeader().getSchemaName(), entry.getEntryHeader().getTableName()}, ".");
                if (heartbeatNs.equalsIgnoreCase(namespace)) {
                    // 如果上一条消息不是心跳，则发送map中的数据清空map
                    if (!lastElemIsHeartbeat && !map.isEmpty()) {
                        doSend(map);
                        map.clear();
                    }
                    lastElemIsHeartbeat = true;
                    MsgColumn msgCol = entry.getMsgColumn();
                    for (RowData rowData : msgCol.getRowDataLst()) {
                        PairWrapper<String, Object> wrapper = Convertor.convertProtobufRecord(entry.getEntryHeader(), rowData);
                        String key = Joiner.on(".").join(wrapper.getPairValue("SCHEMA_NAME").toString(), wrapper.getPairValue("TABLE_NAME").toString());
                        key = key + Constants.EmitFields.HEARTBEAT_FIELD_SUFFIX;
                        put(map, key, key, wrapper, Command.HEART_BEAT);
                    }
                } else {
                    // 如果上一条消息是心跳则发送map中的数据，并且清空map
                    if (lastElemIsHeartbeat && !map.isEmpty()) {
                        doSend(map);
                        map.clear();
                    }
                    lastElemIsHeartbeat = false;
                    if (entry.isDdl()) {
                        String key = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(), entry.getEntryHeader().getTableName());
                        put(map, key, key, entry, Command.META_SYNC);
                    } else {
                        /**
                         * 用于storm分组的table名，对于分区表，如果使用逻辑表名进行分组，可能会出现如下问题:当一个事务中同时操作多个分区表时，这些分区表的增量数据会分组到同一个topic，
                         * 并且一次emit到kafka_write_bolt，为了区分这些分区表的数据，则需要修改kafka_write_bolt的逻辑。因此在这里修改分组的逻辑，如果为分区表，则按物理表名进行分组，
                         * 否则按逻辑表名进行分组。
                         * 为了使不同的物理表分组到同一topic上，需要修改新增一个变量fieldKey
                         */
                        String tableName;
                        if (entry.getEntryHeader().getPartitionTableName().equals(Constants.PARTITION_TABLE_DEFAULT_NAME)) {
                            tableName = entry.getEntryHeader().getTableName();
                        } else {
                            tableName = entry.getEntryHeader().getPartitionTableName();
                        }
                        String key = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(), tableName);
                        String fieldKey = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(), entry.getEntryHeader().getTableName());
                        put(map, key, fieldKey, entry, defaultCmd);
                    }
                }

            } catch (Exception e) {
                logger.warn("Dispatcher error! {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        // 发送最后一包数据
        doSend(map);
    }

    private void doSend(Map<String, GroupData> map) {
        for (Map.Entry<String, GroupData> entry : map.entrySet()) {
            EmitData data = new EmitData();
            GroupData gd = entry.getValue();
            String fieldKey = gd.getFieldKey();
            data.add(EmitData.MESSAGE, gd.getData());
            data.add(EmitData.OFFSET, emitData.get(EmitData.OFFSET));
            data.add(EmitData.GROUP_KEY, fieldKey);
            if (gd.cmd == Command.HEART_BEAT) {
                this.emitHeartbeat(listener.getOutputCollector(), tuple, fieldKey, data, gd.cmd);
            } else {
                this.emit(listener.getOutputCollector(), tuple, fieldKey, data, gd.cmd);
            }
        }
    }

    private void put(Map<String, GroupData> map, String key, String fieldKey, Object value, Command cmd) {
        GroupData data;
        if (map.containsKey(key)) {
            data = map.get(key);
        } else {
            data = new GroupData(cmd);
            map.put(key, data);
        }
        data.add(value);
        data.setFieldKey(fieldKey);
    }

    private class GroupData {
        List<Object> data = new ArrayList<>();
        Command cmd = Command.UNKNOWN_CMD;
        String fieldKey;

        GroupData(Command cmd) {
            this.cmd = cmd;
        }

        void add(Object v) {
            data.add(v);
        }

        List<Object> getData() {
            return data;
        }

        void setFieldKey(String fieldKey) {
            this.fieldKey = fieldKey;
        }

        String getFieldKey() {
            return fieldKey;
        }
    }
}
