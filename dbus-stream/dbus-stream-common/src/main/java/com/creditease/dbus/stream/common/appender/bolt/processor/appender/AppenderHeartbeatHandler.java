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


package com.creditease.dbus.stream.common.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/7/1.
 */
public class AppenderHeartbeatHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public AppenderHeartbeatHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        String groupKey = data.get(EmitData.GROUP_KEY);
        String key = groupKey.substring(0, groupKey.length() - ".heartbeat".length());
        DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key);
        if (table != null) {
            MetaVersion ver = MetaVerController.getVersionFromCache(table.getSchema(), table.getTableName());
            data.add(EmitData.DATA_TABLE, table);
            data.add(EmitData.VERSION, ver);

            this.emitHeartbeat(listener.getOutputCollector(), tuple, groupKey, data, Command.HEART_BEAT);
            if (logger.isDebugEnabled()) {
                Object offset = data.get(EmitData.OFFSET);
                logger.debug("[heartbeat] {} offset:{}", groupKey, offset == null ? -1 : offset);
            }
        } else {
            logger.warn("table[{}] not found in database.", key);
        }
    }

    public static void main(String[] args) {
        String groupKey = "123.heartbeat";
        String key = groupKey.substring(0, groupKey.length() - ".heartbeat".length());
        System.out.println(key);
    }
}
