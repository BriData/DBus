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

import avro.shaded.com.google.common.collect.Lists;
import com.creditease.dbus.stream.common.appender.spout.processor.*;

import java.util.List;

/**
 * Created by Shrimp on 16/8/23.
 */
public class OracleMessageHandler extends AbstractMessageHandler {

    private RecordProcessor<String, byte[]> defaultProcessor;
    private RecordProcessor<String, byte[]> controlProcessor;
    private RecordProcessor<String, byte[]> avroSchemaProcessor;
    private RecordProcessor<String, byte[]> initialLoadProcessor;
    private List<String> schemaTopics;

    public OracleMessageHandler(RecordProcessListener rpListener, ConsumerListener consumerListener) {
        super(rpListener, consumerListener);
        this.schemaTopics = Lists.newArrayList(consumerListener.getSchemaTopics());
        createProcessors();
    }

    private void createProcessors() {
        this.defaultProcessor = new DefaultProcessor(listener, consumerListener);
        this.avroSchemaProcessor = new AvroSchemaProcessor(listener, consumerListener);
        this.controlProcessor = new CtrlEventProcessor(listener, consumerListener);
        this.initialLoadProcessor = new InitialLoadProcessor(controlTopics, listener, consumerListener);
    }

    @Override
    protected RecordProcessor<String, byte[]> chooseProcessor(String recordKey, String topic) {
        if (recordKey != null && recordKey.startsWith(initialLoadTableNs)) {
            return initialLoadProcessor;
        }
        if (controlTopics.contains(topic)) {
            return controlProcessor;
        }
        if (schemaTopics.contains(topic)) {
            return avroSchemaProcessor;
        }
        return defaultProcessor;
    }
}
