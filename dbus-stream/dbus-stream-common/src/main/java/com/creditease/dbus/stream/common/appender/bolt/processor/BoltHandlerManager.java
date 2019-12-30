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

import avro.shaded.com.google.common.collect.Maps;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;

import java.util.Map;

/**
 * Bolt 命令处理器
 * Created by Shrimp on 16/7/1.
 */
public class BoltHandlerManager {

    private Map<Command, BoltCommandHandler> handlers;
    private BoltCommandHandler defaultHandler;
    private BoltCommandHandlerProvider provider;

    private DbusDatasourceType datasourceType;

    public BoltHandlerManager(BoltCommandHandlerProvider provider) {
        this.provider = provider;
        this.datasourceType = GlobalCache.getDatasourceType();
        this.handlers = Maps.newHashMap();
        this.buildCommandHandlers();
    }

    public BoltCommandHandler getHandler(Command cmd) {
        if (handlers.containsKey(cmd)) {
            return handlers.get(cmd);
        }

        if (defaultHandler != null) return defaultHandler;

        throw new RuntimeException("No command handler was found by key:" + cmd);
    }

    private void buildCommandHandlers() {
        Map<Command, BoltCommandHandler> handlers = provider.provideHandlers(datasourceType);
        for (Map.Entry<Command, BoltCommandHandler> entry : handlers.entrySet()) {
            this.addHandler(entry.getKey(), entry.getValue());
        }
        this.setDefaultHandler(provider.provideDefaultHandler(datasourceType));
    }

    private void addHandler(Command cmd, BoltCommandHandler handler) {
        if (!handlers.containsKey(cmd)) {
            handlers.put(cmd, handler);
        }
    }

    private void setDefaultHandler(BoltCommandHandler handler) {
        this.defaultHandler = handler;
    }
}
