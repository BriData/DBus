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


package com.creditease.dbus.stream.mysql.appender.spout.processor;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractProcessor;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;
import com.creditease.dbus.stream.mysql.appender.protobuf.parser.BinlogProtobufParser;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlDefaultProcessor extends AbstractProcessor {
    private final BinlogProtobufParser parser;

    public MysqlDefaultProcessor(RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        parser = BinlogProtobufParser.getInstance();
    }

    @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        try {
            logger.debug("[BEGIN] Receive data,offset:{}", record.offset());
            List<MessageEntry> msgEntryLst = parser.getEntry(record.value());
            dataFilter(msgEntryLst);
            if (!msgEntryLst.isEmpty()) {
                EmitData data = new EmitData();
                data.add(EmitData.OFFSET, record.offset());
                data.add(EmitData.GENERIC_DATA_LIST, msgEntryLst);
                this.listener.emitData(data, Command.UNKNOWN_CMD, record);
                logger.debug("[END] emit data,offset:{}", record.offset());
            } else {
                logger.info("Table not configured in t_dbus_tables {}", Joiner.on(",").join(msgEntryLst.stream()
                        .map(msgEntry -> msgEntry.getEntryHeader().getTableName()).collect(Collectors.toList())));
                listener.reduceFlowSize(record.serializedValueSize());
                // 当前记录不需要处理则直接commit
                consumerListener.syncOffset(record);
            }
            msgEntryLst = null;
        } catch (Exception e) {
            listener.reduceFlowSize(record.serializedValueSize());
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void dataFilter(List<MessageEntry> msgEntryLst) {
        // 过滤掉没有在系统中配置的表或者没有经过拉全量的表的数据
        for (Iterator<MessageEntry> it = msgEntryLst.iterator(); it.hasNext(); ) {
            EntryHeader header = it.next().getEntryHeader();
            String schemaName = header.getSchemaName();
            String tableName = header.getTableName();
            String key = StringUtils.join(new String[]{schemaName, tableName}, ".");
            if (ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key) == null) {
                it.remove();
                logger.info("The message of {} was filtered, the data table is not configured", tableName);
            }
        }
    }
}
