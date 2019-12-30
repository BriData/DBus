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

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/7/1.
 */
public class CommonHeartbeatHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public CommonHeartbeatHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        String groupId = tuple.getStringByField(Constants.EmitFields.GROUP_FIELD);
        this.emitHeartbeat(listener.getOutputCollector(), tuple, groupId, data, Command.HEART_BEAT);
        if (logger.isDebugEnabled()) {
            Object offset = data.get(EmitData.OFFSET);
            logger.debug("[heartbeat] {} {} offset:{}", listener.getClass().getSimpleName(), groupId, offset == null ? -1 : offset);
        }
    }
}
