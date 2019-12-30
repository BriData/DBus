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


package com.creditease.dbus.stream.mongo.appender.bolt.processor.kafkawriter;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.CommonHeartbeatHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.HeartBeatPacket;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.HeartbeatPulse;
import com.creditease.dbus.stream.common.appender.bolt.processor.stat.TableMessageStatReporter;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ximeiwang on 2017/12/28.
 */
public class MongoHeartbeatHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private TableMessageStatReporter reporter;
    private CommonHeartbeatHandler handler;

    public MongoHeartbeatHandler(CommandHandlerListener listener, TableMessageStatReporter reporter) {
        this.listener = listener;
        this.reporter = reporter;
        this.handler = new CommonHeartbeatHandler(listener);
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        String message = data.get(EmitData.MESSAGE);
        if (message != null && !message.isEmpty()) {
            Map<String, Object> wrapper = new HashMap<>();
            JSONObject messageJson = JSONObject.parseObject(message);
            for (String key : messageJson.keySet()) {
                wrapper.put(key, messageJson.getString(key));
            }
            //HeartbeatPulse pulse = HeartbeatPulse.build(wrapper.pairs2map());
            HeartbeatPulse pulse = HeartbeatPulse.build(wrapper);
            if (logger.isDebugEnabled()) {
                Object offset = data.get(EmitData.OFFSET);
                HeartBeatPacket packet = HeartBeatPacket.parse(pulse.getPacket());
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String groupId = tuple.getStringByField(Constants.EmitFields.GROUP_FIELD);
                logger.debug("[heartbeat] {} offset:{} ts:{}, time:{}", groupId, offset == null ? -1 : offset, packet.getTxtime(), format.format(new Date(packet.getTxtime())));
            }
            reporter.mark(pulse);
        }
        handler.handle(tuple);
        this.listener.getOutputCollector().ack(tuple);
    }
}
