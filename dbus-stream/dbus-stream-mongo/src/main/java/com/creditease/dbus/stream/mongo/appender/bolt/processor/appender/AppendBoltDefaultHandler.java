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


package com.creditease.dbus.stream.mongo.appender.bolt.processor.appender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 正常数据处理: 因为mongo没有meta和version的概念，因此这里不对其进行获取meta和version的操作
 * Created by ximeiwang on 2017/12/22.
 */
public class AppendBoltDefaultHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public AppendBoltDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple input) {
        EmitData emitData = (EmitData) input.getValueByField(Constants.EmitFields.DATA);
        Command emitCmd = (Command) input.getValueByField(Constants.EmitFields.COMMAND);
        String message = emitData.get(EmitData.MESSAGE);
        try {
            long offset = emitData.get(EmitData.OFFSET);
            logger.debug("[BEGIN] receive data,offset:{}", offset);

            if (message == null || message.isEmpty()) {
                return;
            }
            JSONObject entryJson = JSON.parseObject(message);
            String namespace = entryJson.getString("_ns");
            String dbSchema = namespace.split("\\.")[0];
            String tableName = namespace.split("\\.")[1];
            if (!filter(dbSchema, tableName)) {
                logger.info("The data of table [{}.{}] have bean aborted", dbSchema, tableName);
                return;
            }
            //TODO 因为Mongo没有meta和version的概念，因此不对获取meta及其version进行任何处理
            //TODO 将数据 emit 到下一个bolt处理
            //TODO 2.获取version以及meta信息
            //long pos = Long.parseLong(msgEntry.getEntryHeader().getPos());
            //MetaVersion version = MetaVerController.getSuitableVersion(dbSchema, tableName, pos, offset);
            MetaVersion version = MetaVerController.getSuitableVersion(dbSchema, tableName, 0, offset);
            if (version == null) {
                // topology中的spout、和上级bolt存在table和version是否存在的验证，正常逻辑不会执行到这里
                logger.error("table[{}.{}] or version not found, data was ignored.", dbSchema, tableName);
                return;
            }
            DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(version.getSchema(), version.getTable()));

            //DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(dbSchema, tableName));
            emitData = new EmitData();
            emitData.add(EmitData.VERSION, version);
            emitData.add(EmitData.OFFSET, offset);
            emitData.add(EmitData.MESSAGE, message);
            emitData.add(EmitData.DATA_TABLE, table);
            this.emit(listener.getOutputCollector(), input, groupField(dbSchema, tableName), emitData, emitCmd);

        } catch (Exception e) {
            logger.error("Error when processing command: {}, input: {}", emitCmd.name(), input, e);
            throw new RuntimeException(e);
        }
    }

    private boolean filter(String dbSchema, String tableName) {
        DataTable dataTable = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(dbSchema, tableName));
        if (dataTable == null) {
            return false;
        } else if ((dbSchema + "." + tableName).equalsIgnoreCase(PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.HEARTBEAT_SRC))) {
            return true;
        } else {
            return !dataTable.isAbort();
        }
    }

}
