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


package com.creditease.dbus.stream.mongo.appender.bolt.processor.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.mongo.appender.convertor.Convertor;
import com.google.common.base.Joiner;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by ximeiwang on 2017/12/22.
 */
public class DispatcherDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    private Tuple tuple;
    private EmitData emitData;

    public DispatcherDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        this.tuple = tuple;
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        this.emitData = emitData;
        String entry = emitData.get(EmitData.GENERIC_DATA_LIST);
        Command cmd = (Command) tuple.getValueByField(Constants.EmitFields.COMMAND);
        String heartbeatNs = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.HEARTBEAT_SRC);
        groupSend(entry, heartbeatNs, cmd);
    }

    private void groupSend(String entry, String heartbeatNs, Command defaultCmd) {
        try {
            // 判断是否为心跳表的数据,如果是心跳表的数据则按照心跳表的schema_name和table_name进行分发
            JSONObject entryJson = JSON.parseObject(entry);
            String namespace = entryJson.getString("_ns");   //TODO 当时DDL语句时，这样获取的namespace貌似就是table就不对，可以考虑在extractor中规范一下，这里就不用进行特殊的处理了
            if (heartbeatNs.equalsIgnoreCase(namespace)) {
                //如果消息是heartbeat数据
                PairWrapper<String, Object> wrapper = Convertor.convertProtobufRecord(entry, entryJson.getString("_o"));
                String key = Joiner.on(".").join(wrapper.getPairValue("SCHEMA_NAME").toString(), wrapper.getPairValue("TABLE_NAME").toString());
                key = key + Constants.EmitFields.HEARTBEAT_FIELD_SUFFIX;

                EmitData data = new EmitData();
                //data.add(EmitData.MESSAGE, document.toString());
                List<Object> message = new ArrayList<>();
                message.add(wrapper);
                data.add(EmitData.MESSAGE, message);
                data.add(EmitData.OFFSET, emitData.get(EmitData.OFFSET));
                data.add(EmitData.GROUP_KEY, key);
                this.emitHeartbeat(listener.getOutputCollector(), tuple, key, data, Command.HEART_BEAT);
            } else {
                //如果数据是正常数据
                ////判断是否是command handler相关数据
                String operation = entryJson.getString("_op");
                if (operation.equals("c")) {
                    // if(namespace.endsWith("$cmd")){ }else{ } //TODO 当dropdatabase时，无法获取table信息，namespace以$cmd结尾
                    String key = namespace;
                    EmitData data = new EmitData();
                    data.add(EmitData.MESSAGE, entry);
                    data.add(EmitData.OFFSET, emitData.get(EmitData.OFFSET));
                    data.add(EmitData.GROUP_KEY, key);
                    this.emit(listener.getOutputCollector(), tuple, key, data, Command.META_SYNC);

                    //String key = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(), entry.getEntryHeader().getTableName());
                    //put(map, key, key, entry, Command.META_SYNC);
                } else {
                    //如果数据是DML信息，即insert、update、remove操作
                    /**
                     * 用于storm分组的table名，对于分区表，如果使用逻辑表名进行分组，可能会出现如下问题:当一个事务中同时操作多个分区表时，这些分区表的增量数据会分组到同一个topic，
                     * 并且一次emit到kafka_write_bolt，为了区分这些分区表的数据，则需要修改kafka_write_bolt的逻辑。因此在这里修改分组的逻辑，如果为分区表，则按物理表名进行分组，
                     * 否则按逻辑表名进行分组。
                     * 为了使不同的物理表分组到同一topic上，需要修改新增一个变量fieldKey
                     * */
                    //TODO 是否Sharding的时候，会否有partition概念呢，现在先不在这里处理
                    /*String tableName;
                    if(entry.getEntryHeader().getPartitionTableName().equals(Constants.PARTITION_TABLE_DEFAULT_NAME)){
                        tableName = entry.getEntryHeader().getTableName();
                    } else {
                        tableName = entry.getEntryHeader().getPartitionTableName();
                    }
                    String key = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(),tableName);
                    String fieldKey = Joiner.on(".").join(entry.getEntryHeader().getSchemaName(),entry.getEntryHeader().getTableName());
                    */
                    String fieldKey = namespace;
                    EmitData data = new EmitData();
                    data.add(EmitData.MESSAGE, entry);
                    data.add(EmitData.OFFSET, emitData.get(EmitData.OFFSET));
                    data.add(EmitData.GROUP_KEY, fieldKey);
                    this.emit(listener.getOutputCollector(), tuple, fieldKey, data, Command.UNKNOWN_CMD);
                }
            }
        } catch (Exception e) {
            logger.warn("Dispatcher error! data:{}", JSON.toJSONString(entry), e);
            throw new RuntimeException(e);
        }
    }
}
