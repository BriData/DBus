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


package com.creditease.dbus.stream.common.appender.bolt.processor.stat;

import com.creditease.dbus.commons.StatMessage;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Shrimp on 16/8/26.
 */
public class TableMessageStatReporter {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final String STAT_TYPE = "APPENDER_TYPE";
    private static final String CHECKPOINT_FLAG = "checkpoint";
    private Map<String, StatMessage> statMessageMap;
    private StatSender sender;

    public TableMessageStatReporter(StatSender sender) {
        this.sender = sender;
        this.statMessageMap = new HashMap<>();
    }

    public void reportStat(String key) {
        reportStat(key, 1);
    }

    public void reportStat(String key, int count) {
        if (!statMessageMap.containsKey(key)) {
            logger.warn("key[{}] not found, please check heartbeat.", key);
            return;
        }
        logger.debug("reportStat key:{},count:{}", key, count);
        StatMessage m = statMessageMap.get(key);
        m.addCount(count);
    }

    public void mark(HeartbeatPulse pulse) {
        // 判断心跳类型是否为checkpoint
        if (!pulse.getPacket().contains(CHECKPOINT_FLAG)) {
            return;
        }

        String key = Joiner.on(".").join(pulse.getSchemaName(), pulse.getTableName());
        logger.debug("mark: {}", key);
        StatMessage message;
        if (!statMessageMap.containsKey(key)) {
            message = new StatMessage(pulse.getDsName(), pulse.getSchemaName(), pulse.getTableName(), STAT_TYPE);
            statMessageMap.put(key, message);
        } else {
            message = statMessageMap.get(key);
        }

        HeartBeatPacket packet = HeartBeatPacket.parse(pulse.getPacket());
        message.setCheckpointMS(packet.getTime());
        message.setTxTimeMS(packet.getTxtime());
        message.setLocalMS(System.currentTimeMillis());
        message.setLatencyMS(message.getLocalMS() - message.getCheckpointMS());
        sender.sendStat(message.toJSONString(), message.getDsName(), message.getSchemaName(), message.getTableName());
        message.cleanUp();
    }
}
