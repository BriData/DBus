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


package com.creditease.dbus.stream.mongo.appender.spout.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractProcessor;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;
import com.creditease.dbus.stream.mongo.appender.parser.OplogParser;

/**
 * Created by ximeiwang on 2017/12/19.
 */
public class MongoDefaultProcessor extends AbstractProcessor {
    private final OplogParser parser;

    public MongoDefaultProcessor(RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        parser = OplogParser.getInstance();
    }

    @Override
    // public void process(ConsumerRecord<String, byte[]> record, Object... args) {
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        try {
            logger.debug("[BEGIN] Receive data,offset:{}", record.offset());
            String entry = parser.getEntry(record.value());
            JSONObject entryJson = JSON.parseObject(entry);

            boolean filterFlag = dataFilter(entry);

            //if(!entry.isEmpty()){
            if (filterFlag == true) {
                EmitData data = new EmitData();
                data.add(EmitData.OFFSET, record.offset());
                data.add(EmitData.GENERIC_DATA_LIST, entry);
                //List<Object> values = new Values(data, Command.UNKNOWN_CMD);
                this.listener.emitData(data, Command.UNKNOWN_CMD, record);
                logger.debug("[END] emit data,offset:{}", record.offset());
            } else {
                //logger.info("Table not configured in t_dbus_tables {}", Joiner.on(",").join(msgEntryLst.stream()
                //.map(msgEntry->msgEntry.getEntryHeader().getTableName()).collect(Collectors.toList())));
                logger.info("Table not configured in t_dbus_tables {}", entryJson.getString("_ns").split("\\.")[1]);
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

    /*
     ** 过滤掉没有在系统中配置的表或者没有经过拉全量的表的数据
     */
    private boolean dataFilter(String entry) {
        JSONObject entryJson = JSON.parseObject(entry);
        String key = entryJson.getString("_ns");
        if (ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key) == null) {
            logger.info("The message of {} was filtered, the data table is not configured", key.split("\\.")[1]);
            return false;
        }
        return true;
    }

   /* @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {

    }*/
}
