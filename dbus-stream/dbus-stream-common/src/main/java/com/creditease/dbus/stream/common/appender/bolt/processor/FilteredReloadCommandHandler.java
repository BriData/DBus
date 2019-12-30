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

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.TimeUnit;

/**
 * 重新加载命令处理器,重新加载会发送给所有的下游bolt
 * Created by zhangyf on 17/3/14.
 */
public class FilteredReloadCommandHandler implements BoltCommandHandler {
    private CommandHandlerListener listener;
    private BoltCommandHandler handler;
    private Cache<Long, Object> cache;

    public FilteredReloadCommandHandler(CommandHandlerListener listener) {
        this.listener = listener;
        handler = new CommonReloadHandler(listener);
        // 将cache清理时间缩短，避免有些线程无法reload的bug
        cache = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.SECONDS).build();
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        ControlMessage message = emitData.get(EmitData.MESSAGE);
        Object obj = cache.getIfPresent(message.getId());
        if (obj != null) {
            return;
        }
        cache.put(message.getId(), new Object());
        handler.handle(tuple);
    }
}
