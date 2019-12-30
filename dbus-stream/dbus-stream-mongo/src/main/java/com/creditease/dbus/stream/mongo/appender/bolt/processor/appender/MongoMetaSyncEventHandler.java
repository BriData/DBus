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
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaEventWarningSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ximeiwang on 2018/1/8.
 * handle command handle information, DDL: create/drop/rename collection/drop database, so far just this four class
 */
public class MongoMetaSyncEventHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private MetaEventWarningSender sender;

    public MongoMetaSyncEventHandler(CommandHandlerListener listener) {
        this.listener = listener;
        this.sender = new MetaEventWarningSender();
    }

    @Override
    public void handle(Tuple tuple) {
        try {
            EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
            String entry = emitData.get(EmitData.MESSAGE);
            long offset = emitData.get(EmitData.OFFSET);
            logger.debug("[BEGIN] receive data,offset:{}", offset);

            //long pos = Long.parseLong(msgEntry.getEntryHeader().getPos()); //TODO 不知道Mongo的pos是多少
            syncMeta(entry, 0, offset, tuple);
        } catch (Exception e) {
            logger.error("Error when processing data", e);
            throw new RuntimeException(e);
        }
    }

    private void syncMeta(String entry, long pos, long offset, Tuple input) throws Exception {
        JSONObject entryJson = JSON.parseObject(entry);
        String document = entryJson.getString("_o");
        JSONObject docJson = JSON.parseObject(document);
        String namespace = entryJson.getString("_ns");

        if (!document.isEmpty())
            logger.info("Received a meta sync message {}", document);
        else return;
        if (docJson.get("dropDatabase") != null) {
            logger.error("Drop database {}.", namespace.split("\\.")[0]);
        } else {
            //handle create/drop/rename collection
            String schemaName = namespace.split("\\.")[0];
            String tableName = namespace.split("\\.")[1];
            /*MetaVersion version = MetaVerController.getVersionFromCache(schemaName, tableName);
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", schemaName, tableName);
                return;
            }*/
            // 修改data table表状态
            BoltCommandHandlerHelper.changeDataTableStatus(schemaName, tableName, DataTable.STATUS_ABORT);

        }
    }
}
