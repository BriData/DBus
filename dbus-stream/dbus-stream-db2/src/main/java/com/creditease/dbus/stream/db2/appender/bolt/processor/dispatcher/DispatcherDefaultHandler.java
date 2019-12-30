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


package com.creditease.dbus.stream.db2.appender.bolt.processor.dispatcher;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.db2.appender.avro.GenericData;
import com.creditease.dbus.stream.db2.appender.avro.GenericSchemaDecoder;
import com.creditease.dbus.stream.db2.appender.avro.SchemaProvider;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.shade.com.codahale.metrics.Meter;
import org.apache.storm.shade.com.codahale.metrics.MetricRegistry;
import org.apache.storm.shade.com.codahale.metrics.ScheduledReporter;
import org.apache.storm.shade.com.codahale.metrics.Slf4jReporter;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shrimp on 16/7/1.
 */
public class DispatcherDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private static final String HEARTBEAT_NS = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.HEARTBEAT_SRC);

    private Tuple tuple;
    private EmitData emitData;
    private MetricReporter reporter = new MetricReporter();

    public DispatcherDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }


    @Override
    public void handle(Tuple tuple) {
        this.tuple = tuple;
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        this.emitData = emitData;
        List<GenericData> dataList = emitData.get(EmitData.GENERIC_DATA_LIST);
        groupSend(dataList);
    }

    private void groupSend(List<GenericData> dataList) {
        Map<String, GroupData> map = Maps.newLinkedHashMap();
        Command cmd = (Command) tuple.getValueByField(Constants.EmitFields.COMMAND);
        boolean lastElemIsHeartbeat = false;
        for (GenericData data : dataList) {
            try {
                // 判断是否为心跳表的数据,如果是心跳表的数据则按照心跳表的schema_name和table_name进行分发
                if (HEARTBEAT_NS.equals(data.getTableName())) {
                    // 如果上一条消息不是心跳，则发送map中的数据清空map
                    if (!lastElemIsHeartbeat && !map.isEmpty()) {
                        doSend(map);
                        map.clear();
                    }
                    lastElemIsHeartbeat = true;

                    Schema schema = SchemaProvider.getInstance().getSchemaBySchemaRegistry(data.getTableName(), data.getPayload());
                    //解码心跳数据
                    List<GenericRecord> results = GenericSchemaDecoder.decoder().decode(schema, data.getPayload());
                    for (GenericRecord gr : results) {
                        PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertDB2AvroRecord(gr);
                        String key = Joiner.on(".").join(wrapper.getPairValue("SCHEMA_NAME").toString(), wrapper.getPairValue("TABLE_NAME").toString());
                        put(map, key + Constants.EmitFields.HEARTBEAT_FIELD_SUFFIX, wrapper, Command.HEART_BEAT);
                    }
                } else {
                    // 如果上一条消息是心跳则发送map中的数据，并且清空map
                    if (lastElemIsHeartbeat && !map.isEmpty()) {
                        doSend(map);
                        map.clear();
                    }
                    lastElemIsHeartbeat = false;
                    put(map, data.getTableName(), data, cmd);
                }
            } catch (Exception e) {
                logger.warn("Dispatcher error! data:{}", JSON.toJSONString(data), e);
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
            data.add(EmitData.MESSAGE, gd.getData());
            data.add(EmitData.OFFSET, emitData.get(EmitData.OFFSET));
            data.add(EmitData.GROUP_KEY, entry.getKey());

            if (gd.cmd == Command.HEART_BEAT) {
                this.emitHeartbeat(listener.getOutputCollector(), tuple, entry.getKey(), data, gd.cmd);
                logger.debug("[appender-dispatcher] send heartbeat {} offset:{}", entry.getKey(), data.get(EmitData.OFFSET));
            } else {
                logger.debug("[appender-dispatcher_:" + gd.cmd + "] send group, offset: {}:" + data.get(EmitData.OFFSET));
                this.emit(listener.getOutputCollector(), tuple, entry.getKey(), data, gd.cmd);
            }
        }
    }

    private void put(Map<String, GroupData> map, String key, Object value, Command cmd) {
        GroupData data;
        if (map.containsKey(key)) {
            data = map.get(key);
        } else {
            data = new GroupData(cmd);
            map.put(key, data);
        }
        data.add(value);
        reporter.report();
    }

    private class GroupData {
        List<Object> data = new ArrayList<>();
        Command cmd = Command.UNKNOWN_CMD;

        GroupData(Command cmd) {
            this.cmd = cmd;
        }

        void add(Object v) {
            data.add(v);
        }

        List<Object> getData() {
            return data;
        }
    }

    private class MetricReporter {
        private MetricRegistry metrics = new MetricRegistry();
        private ScheduledReporter reporter = null;
        private Meter messagesMeter = null;

        private MetricReporter() {
            reporter = Slf4jReporter.forRegistry(metrics)
                    .outputTo(LoggerFactory.getLogger("com.creditease.dbus.metrics"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();

            messagesMeter = metrics.meter("A-dispatcher-grpcount");
            reporter.start(10, TimeUnit.SECONDS);
        }

        public void report(int count) {
            messagesMeter.mark(count);
        }

        public void report() {
            messagesMeter.mark(1);
        }
    }
}
