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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.oracle.appender.avro.DBusSchemaWriter;
import com.creditease.dbus.stream.oracle.appender.avro.DefaultSchemaWriter;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 保存接收到的Avro Schema
 * Created by Shrimp on 16/7/1.
 */
public class AvroSchemaHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private DBusSchemaWriter writer;

    @Override
    public void handle(Tuple tuple) {
        try {
            EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
            DBusConsumerRecord<String, byte[]> record = emitData.get(EmitData.MESSAGE);
            saveAvroSchema(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 保存Avro Schema
     */
    private void saveAvroSchema(DBusConsumerRecord<String, byte[]> record) throws Exception {
        String schemaStr = new String(record.value(), "utf-8");
        logger.info("Receive Schema:" + Utils.replaceBlanks(schemaStr));

        Map<String, Object> map = (Map<String, Object>) JSON.parse(schemaStr);
        String tableName = map.get(Constants.MessageBodyKey.TABLE_NAME).toString();
        String namespace = map.get(Constants.MessageBodyKey.NAMESPACE).toString();
        if (writer == null) {
            writer = new DefaultSchemaWriter(true);
        }
        writer.write(schemaStr, namespace, tableName, schemaStr.hashCode());
    }
}
