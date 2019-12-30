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
import com.creditease.dbus.stream.common.appender.bolt.processor.AbsCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.CommonReloadHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.dispatcher.DispatcherInitialLoadHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.dispatcher.DispatcherResumeHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.mysql.appender.bolt.processor.dispatcher.MdDefaultHandler;

import java.util.Map;

/**
 * Created by zhangyf on 17/8/16.
 */
public class DispatcherCmdHandlerProvider extends AbsCommandHandlerProvider {

    public DispatcherCmdHandlerProvider(CommandHandlerListener listener) {
        super(listener);
    }

    @Override
    public Map<Command, BoltCommandHandler> provideHandlers(DbusDatasourceType type) {
        Map<Command, BoltCommandHandler> map = Maps.newHashMap();

        map.put(Command.APPENDER_RELOAD_CONFIG, new CommonReloadHandler(listener));
        map.put(Command.APPENDER_TOPIC_RESUME, new DispatcherResumeHandler(listener));
        map.put(Command.FULL_DATA_PULL_REQ, new DispatcherInitialLoadHandler(listener));
        return map;
    }

    @Override
    public BoltCommandHandler provideDefaultHandler(DbusDatasourceType type) {
        BoltCommandHandler defaultHandler = new MdDefaultHandler(listener);
        return defaultHandler;
    }
}
