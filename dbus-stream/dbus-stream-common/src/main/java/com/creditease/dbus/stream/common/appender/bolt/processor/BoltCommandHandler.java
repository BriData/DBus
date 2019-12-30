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


package com.creditease.dbus.stream.common.appender.bolt.processor;

import com.creditease.dbus.stream.common.Constants.EmitFields;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by Shrimp on 16/7/1.
 */
public interface BoltCommandHandler {

    void handle(Tuple tuple);

    /**
     * 将data数组中的数据,按照groupField分组发送的一个bolt处理,
     * Grouping策略使用DbusGrouping实现
     *
     * @param collector
     * @param tuple      当前bolt接收到的数据
     * @param groupField 分组字段
     * @param data       要发送的数据
     */
    default void emit(OutputCollector collector, Tuple tuple, String groupField, EmitData data, Command cmd) {
        List<Object> values = Lists.newArrayList(groupField, data, cmd, EmitFields.EMIT_TO_BOLT);
        collector.emit(tuple, values);
    }

    /**
     * 将tuple中的数据发送给下游所有的bolt
     *
     * @param collector
     * @param tuple     当前bolt接收到的数据
     */
    default void emitToAll(OutputCollector collector, Tuple tuple, EmitData data, Command cmd) {
        List<Object> values = Lists.newArrayList(EmitData.NO_VALUE, data, cmd, EmitFields.EMIT_TO_ALL);
        collector.emit(tuple, values);
    }

    /**
     * 将data数组中的数据,按照groupField分组发送的一个bolt处理,
     * Grouping策略使用DbusGrouping实现
     *
     * @param collector
     * @param tuple      当前bolt接收到的数据
     * @param groupField 分组字段
     * @param data       要发送的数据
     */
    default void emitHeartbeat(OutputCollector collector, Tuple tuple, String groupField, EmitData data, Command cmd) {
        List<Object> values = Lists.newArrayList(groupField, data, cmd, EmitFields.EMIT_HEARTBEAT);
        collector.emit(tuple, values);
    }

    default String groupField(String schema, String table) {
        return Joiner.on(".").join(schema, table);
    }

}
