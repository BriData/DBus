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


package com.creditease.dbus.stream.oracle.appender.spout.processor;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.spout.processor.AbstractProcessor;
import com.creditease.dbus.stream.common.appender.spout.processor.ConsumerListener;
import com.creditease.dbus.stream.common.appender.spout.processor.RecordProcessListener;

/**
 * 处理接收到的Avro Schema
 * Created by Shrimp on 16/6/21.
 */
public class AvroSchemaProcessor extends AbstractProcessor {

    public AvroSchemaProcessor(RecordProcessListener listener, ConsumerListener consumerListener) {
        super(listener, consumerListener);
    }

    @Override
    public void process(DBusConsumerRecord<String, byte[]> record, Object... args) {
        EmitData data = new EmitData();
        data.add(EmitData.MESSAGE, record);
        listener.emitData(data, Command.AVRO_SCHEMA, record);
    }
}
