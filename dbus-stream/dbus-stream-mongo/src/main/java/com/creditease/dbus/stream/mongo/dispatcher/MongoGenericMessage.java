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


package com.creditease.dbus.stream.mongo.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.stream.common.tools.IGenericMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ximeiwang on 2017/12/7.
 */
public class MongoGenericMessage implements IGenericMessage {

    private String entry;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public MongoGenericMessage(String entry) {
        this.entry = entry;
    }

    @Override
    public String getNameSpace() {
        JSONObject entryJson = JSON.parseObject(entry);
        String namespace = entryJson.getString("_ns");
        return namespace.toString();
    }

    @Override
    public String getSchemaName() {
        JSONObject entryJson = JSON.parseObject(entry);
        String namespace = entryJson.getString("_ns");
        //TODO namespace不合适的字段，先抛弃，返回空
        if (namespace == null || namespace.trim().length() == 0
                || !namespace.contains(".")) {
            logger.info("namespace : {}.  is not available. schemaName will be Empty", namespace);
            return "";
        }
        String schemaName = namespace.split("\\.")[0];
        return schemaName;
    }

    @Override
    public String getTableName() {
        JSONObject entryJson = JSON.parseObject(entry);
        String namespace = entryJson.getString("_ns");
        //TODO namespace不合适的字段，先抛弃，返回空
        if (namespace == null || namespace.trim().length() == 0
                || !namespace.contains(".")) {
            logger.info("namespace : {}.  is not available. tableName will be Empty", namespace);
            return "";
        }
        String tableName = namespace.split("\\.")[1];
        return tableName;
    }


    @Override
    public int getSchemaId() {
        return 0;
    }

    @Override
    public int getSchemaHash() {
        //TODO schemaHash is what?
        return 0;
    }

    @Override
    public int getRowCount() {
        return 1;
        //return 0;
    }

    @Override
    public boolean isDML() {
        JSONObject entryJson = JSON.parseObject(entry);
        String operation = entryJson.getString("_op");
        switch (operation) {
            case "i":
            case "u":
            case "d":
                return true;
            default:
                return false;
        }
    }

    public String getEntry() {
        return entry;
    }
}
