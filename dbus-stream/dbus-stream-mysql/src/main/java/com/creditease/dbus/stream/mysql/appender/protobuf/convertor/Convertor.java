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


package com.creditease.dbus.stream.mysql.appender.protobuf.convertor;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.utils.Pair;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Convertor {
    private static final String FULL_DATA_PULL_REQ = "FULL_DATA_PULL_REQ";

    /**
     * mysql 拉全量请求
     *
     * @return
     */
    public static ControlMessage mysqlFullPullMessage(MessageEntry entry, String topologyId, DBusConsumerRecord<String, byte[]> consumerRecord) {
        ControlMessage message = new ControlMessage();
        message.setId(System.currentTimeMillis());
        message.setFrom(topologyId);
        message.setType(FULL_DATA_PULL_REQ);
        message.addPayload("topic", consumerRecord.topic());
        message.addPayload("DBUS_DATASOURCE_ID", Utils.getDatasource().getId());
        PairWrapper<String, Object> wrapper = convertProtobufRecord(entry.getEntryHeader(), entry.getMsgColumn().getRowDataLst().get(0));
        message.addPayload("OP_TS", entry.getEntryHeader().getTsTime());
        message.addPayload("POS", entry.getEntryHeader().getPos());
        for (Pair<String, Object> pair : wrapper.getPairs()) {
            message.addPayload(pair.getKey().toUpperCase(), pair.getValue());
        }
        return message;
    }

    public static <T extends Object> PairWrapper<String, Object> convertProtobufRecord(EntryHeader header, RowData rowData) {
        PairWrapper<String, Object> wrapper = new PairWrapper<>();
        List<Column> Columns = null;
        if (header.isInsert() || header.isUpdate()) {
            Columns = rowData.getAfterColumnsList();
        } else if (header.isDelete()) {
            Columns = rowData.getBeforeColumnsList();
        }
        wrapper.addProperties(Constants.MessageBodyKey.POS, header.getPos());
        wrapper.addProperties(Constants.MessageBodyKey.OP_TS, header.getTsTime());

        Map<String, Object> map = convert2map(Columns);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            wrapper.addPair(new Pair<>(entry.getKey(), CharSequence.class.isInstance(entry.getValue()) ? entry.getValue().toString() : entry.getValue()));
        }

        return wrapper;
    }

    public static <T extends Object> PairWrapper<String, Object> convertProtobufRecordBeforeUpdate(EntryHeader header, RowData rowData) {
        PairWrapper<String, Object> wrapper = new PairWrapper<>();
        List<Column> Columns = rowData.getBeforeColumnsList();
        Map<String, Object> map = convert2map(Columns);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            wrapper.addPair(new Pair<>(entry.getKey(), CharSequence.class.isInstance(entry.getValue()) ? entry.getValue().toString() : entry.getValue()));
        }

        return wrapper;
    }

    private static Map<String, Object> convert2map(List<Column> cols) {
        Map<String, Object> map = Maps.newHashMap();
        if (cols != null) {
            for (Column col : cols) {
                String colName = col.getName();
                if (col.getIsNull()) {
                    map.put(colName, null);
                } else {
                    map.put(colName, col.getValue());
                }
            }
        }
        return map;
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("1", "women");
        map.put("2", "man");
        map.put("3", null);
        map.put("4", null);
        map.put("5", null);

        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println("key:" + entry.getKey() + ", value:" + entry.getValue());
            if (CharSequence.class.isInstance(entry.getValue())) {
                System.out.println(true);
            }
        }
    }
}
