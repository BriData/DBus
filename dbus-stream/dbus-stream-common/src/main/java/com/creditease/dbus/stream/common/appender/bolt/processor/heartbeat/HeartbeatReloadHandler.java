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


package com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat;

import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 重新加载命令处理器,重新加载会发送给所有的下游bolt
 * Created by Shrimp on 16/7/1.
 */
public class HeartbeatReloadHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private long lastReloadTs = 0L;
    private final long MIN_PERIOD_MS = 20 * 1000;

    public HeartbeatReloadHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        long ts = System.currentTimeMillis();
        if (ts - lastReloadTs > MIN_PERIOD_MS) {
            listener.reloadBolt(tuple);
            lastReloadTs = ts;
            logger.info("Heartbeat bolt reloaded, reload ts:{}", lastReloadTs);
            return;
        }
        logger.debug("Bolt has been reloaded at " + new Date(lastReloadTs));
    }
}
