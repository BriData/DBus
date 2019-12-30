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


package com.creditease.dbus.stream.common.appender.spout.processor;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.cmds.CtrlCommand;
import com.creditease.dbus.stream.common.appender.spout.cmds.TopicResumeCmd;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


/**
 * 处理control topic事件的 processor
 * Created by Shrimp on 16/6/21.
 */
public class CtrlEventProcessor extends AbstractProcessor {

    private final int CACHE_EXPIRE_TIME = 1;
    private Cache<Object, Object> cache;

    public CtrlEventProcessor(RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(CACHE_EXPIRE_TIME, TimeUnit.HOURS).build();
    }

    @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        try {
            processControlEvent(record);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void processControlEvent(DBusConsumerRecord<String, byte[]> record) throws Exception {
        Object processed = cache.getIfPresent(record.offset());
        if (processed == null) {
            String key = record.key();
            Command cmd = Command.parse(key);
            String event = new String(record.value(), "utf-8");
            ControlMessage message = ControlMessage.parse(event);
            logger.info("Received a ControlMessage {key:{}, event:{}}", key, event);
            switch (cmd) {
                case APPENDER_TOPIC_RESUME:

                    final CtrlCommand ctrlCmd = CtrlCommand.parse(Command.APPENDER_TOPIC_RESUME, message);
                    EmitData emitData = ctrlCmd.execCmd((cmd1, args) -> {
                        TopicResumeCmd rCmd = (TopicResumeCmd) cmd1;

                        consumerListener.resumeTopic(rCmd.getTopic(), message);

                        EmitData data = new EmitData();
                        data.add(EmitData.MESSAGE, message);
                        data.add(EmitData.CTRL_CMD, rCmd);

                        return data;
                    });

                    // 发送命令给bolt,开始发送数据,停止过滤termination的表
                    listener.emitData(emitData, Command.APPENDER_TOPIC_RESUME, record);
                    return; // 不用继续执行

                case APPENDER_RELOAD_CONFIG:

                    listener.reduceFlowSize(record.serializedValueSize());
                    listener.markReloading(record, Collections.singletonMap("message", message));
                    consumerListener.syncOffset(record);
                    break;

                case PAUSE_APPENDER_DATA_TOPIC:
                    listener.reduceFlowSize(record.serializedValueSize());
                    consumerListener.pauseAppender();
                    consumerListener.syncOffset(record);
                    logger.info("All of the data topics are paused");
                    break;
                case RESUME_APPENDER:
                    listener.reduceFlowSize(record.serializedValueSize());
                    consumerListener.resumeAppender();
                    consumerListener.syncOffset(record);
                    logger.info("All of the data topics are resumed");
                    break;
                default:
                    listener.reduceFlowSize(record.serializedValueSize());
                    consumerListener.syncOffset(record);
                    break;
            }
        } else {
            listener.reduceFlowSize(record.serializedValueSize());
            consumerListener.syncOffset(record);
            logger.info("Data have bean processed->{}", record.toString());
        }
    }
}
