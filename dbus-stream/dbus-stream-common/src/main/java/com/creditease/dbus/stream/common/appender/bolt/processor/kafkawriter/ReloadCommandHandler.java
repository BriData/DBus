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


package com.creditease.dbus.stream.common.appender.bolt.processor.kafkawriter;

import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.FilteredReloadCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import org.apache.storm.tuple.Tuple;

/**
 * 重新加载命令处理器,重新加载会发送给所有的下游bolt
 * Created by Shrimp on 16/7/1.
 */
public class ReloadCommandHandler implements BoltCommandHandler {
    private CommandHandlerListener listener;
    private BoltCommandHandler handler;

    public ReloadCommandHandler(CommandHandlerListener listener) {
        this.listener = listener;
        this.handler = new FilteredReloadCommandHandler(listener);
    }

    @Override
    public void handle(Tuple tuple) {
        handler.handle(tuple);
        listener.getOutputCollector().ack(tuple);
    }
}
