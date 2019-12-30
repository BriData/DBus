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
import com.creditease.dbus.stream.common.appender.bolt.processor.*;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.mysql.appender.bolt.processor.wrapper.MysqlWrapperDefaultHandler;

import java.util.Map;

/**
 * Created by zhangyf on 17/8/16.
 */
public class WrapperCmdHandlerProvider extends AbsCommandHandlerProvider {

    public WrapperCmdHandlerProvider(CommandHandlerListener listener) {
        super(listener);
    }

    @Override
    public Map<Command, BoltCommandHandler> provideHandlers(DbusDatasourceType type) {
        Map<Command, BoltCommandHandler> map = Maps.newHashMap();
        map.put(Command.HEART_BEAT, new CommonHeartbeatHandler(listener));
        map.put(Command.APPENDER_RELOAD_CONFIG, new FilteredReloadCommandHandler(listener));
        map.put(Command.DATA_INCREMENT_TERMINATION, new CommonTerminationHandler(listener));
        /**
         * 在此处添加新的handler
         * 目的是处理拉全量之后需要重新从zk中获取UMS_UID
         * 否则后续的一段时间之内，增量的UMS_UID将比全量的小
         */
        map.put(Command.APPENDER_TOPIC_RESUME, new WrapperResumeHandler(listener));

        return map;
    }

    @Override
    public BoltCommandHandler provideDefaultHandler(DbusDatasourceType type) {
        BoltCommandHandler defaultHandler = new MysqlWrapperDefaultHandler(listener);
        return defaultHandler;
    }
}
