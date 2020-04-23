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

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat.HeartbeatDefaultHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.HeartbeatHandlerListener;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.HeartBeatPacket;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.HeartbeatPulse;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.TableMessageStatReporter;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by Shrimp on 16/7/1.
 */
public class HeartbeatHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private TableMessageStatReporter reporter;
    private BoltCommandHandler handler;

    public HeartbeatHandler(HeartbeatHandlerListener listener, TableMessageStatReporter reporter) {
        this.listener = listener;
        this.reporter = reporter;
        //this.handler = new CommonHeartbeatHandler(listener);
        this.handler = new HeartbeatDefaultHandler(listener);
    }

    public HeartbeatHandler(HeartbeatHandlerListener listener, TableMessageStatReporter reporter, BoltCommandHandler handler) {
        this.listener = listener;
        this.reporter = reporter;
        this.handler = handler;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        List<PairWrapper<String, Object>> wrapperList = data.get(EmitData.MESSAGE);
        if (wrapperList != null && !wrapperList.isEmpty()) {
            for (PairWrapper<String, Object> wrapper : wrapperList) {
                HeartbeatPulse pulse = HeartbeatPulse.build(wrapper.pairs2map());
                if (logger.isDebugEnabled()) {
                    Object offset = data.get(EmitData.OFFSET);
                    HeartBeatPacket packet = HeartBeatPacket.parse(pulse.getPacket());
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String groupId = tuple.getStringByField(Constants.EmitFields.GROUP_FIELD);
                    logger.debug("[heartbeat] {} offset:{} ts:{}, time:{}", groupId, offset == null ? -1 : offset, packet.getTxtime(), format.format(new Date(packet.getTxtime())));
                }
                reporter.mark(pulse);
            }
        }

        handler.handle(tuple);
        this.listener.getOutputCollector().ack(tuple);
    }
}
