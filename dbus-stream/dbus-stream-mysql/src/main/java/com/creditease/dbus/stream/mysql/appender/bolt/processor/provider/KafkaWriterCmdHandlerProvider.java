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


package com.creditease.dbus.stream.mysql.appender.bolt.processor.provider;

import avro.shaded.com.google.common.collect.Maps;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.kafkawriter.HeartbeatHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.kafkawriter.IncrementTerminationHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.kafkawriter.KafkaBoltDefaultHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.kafkawriter.ReloadCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.KafkaBoltHandlerListener;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.TableMessageStatReporter;
import com.creditease.dbus.stream.common.appender.enums.Command;

import java.util.Map;

/**
 * Created by zhangyf on 17/8/16.
 */
public class KafkaWriterCmdHandlerProvider implements BoltCommandHandlerProvider {

    private KafkaBoltHandlerListener listener;
    private TableMessageStatReporter tableStatReporter;

    public KafkaWriterCmdHandlerProvider(KafkaBoltHandlerListener listener, TableMessageStatReporter tableStatReporter) {
        this.listener = listener;
        this.tableStatReporter = tableStatReporter;
    }

    @Override
    public Map<Command, BoltCommandHandler> provideHandlers(DbusDatasourceType type) {
        Map<Command, BoltCommandHandler> map = Maps.newHashMap();

        map.put(Command.APPENDER_RELOAD_CONFIG, new ReloadCommandHandler(listener));
        map.put(Command.HEART_BEAT, new HeartbeatHandler(listener, tableStatReporter));
        map.put(Command.DATA_INCREMENT_TERMINATION, new IncrementTerminationHandler(listener));

        return map;
    }

    @Override
    public BoltCommandHandler provideDefaultHandler(DbusDatasourceType type) {
        return new KafkaBoltDefaultHandler(listener, tableStatReporter);
    }
}
