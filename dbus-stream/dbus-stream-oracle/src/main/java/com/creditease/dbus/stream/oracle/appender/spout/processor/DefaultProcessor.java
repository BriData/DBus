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


package com.creditease.dbus.stream.oracle.appender.spout.processor;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractProcessor;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;
import com.creditease.dbus.stream.oracle.appender.avro.GenericData;
import com.creditease.dbus.stream.oracle.appender.avro.GenericSchemaDecoder;
import com.google.common.base.Joiner;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 处理control topic事件的 processor
 * Created by Shrimp on 16/6/21.
 */
public class DefaultProcessor extends AbstractProcessor {

    private GenericSchemaDecoder decoder;

    public DefaultProcessor(RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        this.decoder = GenericSchemaDecoder.decoder();
    }

    @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        try {
            logger.debug("[BEGIN] Receive data,offset:{}", record.offset());
            List<GenericData> schemaList = decoder.unwrap(record.value());

            dataFilter(schemaList);

            if (!schemaList.isEmpty()) {

                EmitData data = new EmitData();
                data.add(EmitData.OFFSET, record.offset());
                data.add(EmitData.GENERIC_DATA_LIST, schemaList);
                this.listener.emitData(data, Command.UNKNOWN_CMD, record);
                logger.debug("[END] emit data,offset:{}", record.offset());

            } else {
                logger.info("Table not configured in t_dbus_tables {}", Joiner.on(",").join(schemaList.stream()
                        .map(GenericData::getTableName).collect(Collectors.toList())));
                listener.reduceFlowSize(record.serializedValueSize());
                // 当前记录不需要处理则直接commit
                consumerListener.syncOffset(record);
            }
        } catch (Exception e) {
            listener.reduceFlowSize(record.serializedValueSize());
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void dataFilter(List<GenericData> schemaList) {

        // 过滤掉没有在系统中配置的表或者没有经过拉全量的表的数据
        for (Iterator<GenericData> it = schemaList.iterator(); it.hasNext(); ) {
            String tableName = it.next().getTableName();
            if (ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, tableName) == null) {
                it.remove();
                logger.info("The message of {} was filtered, the data table is not configured", tableName);
            }
        }
    }
}
