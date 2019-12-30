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

import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.KafkaBoltHandlerListener;
import org.apache.storm.tuple.Tuple;

/**
 * Created by Shrimp on 16/7/4.
 */
public class IncrementTerminationHandler implements BoltCommandHandler {
    private KafkaBoltHandlerListener listener;

    public IncrementTerminationHandler(KafkaBoltHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        MetaVersion version = emitData.get(EmitData.VERSION);
        DbusMessage message = emitData.get(EmitData.MESSAGE);
        listener.writeData(version.getSchema(), version.getTable(), message, tuple);
    }
}
