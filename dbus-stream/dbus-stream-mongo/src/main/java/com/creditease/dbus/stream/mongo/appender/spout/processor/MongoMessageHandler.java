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


package com.creditease.dbus.stream.mongo.appender.spout.processor;

import com.creditease.dbus.stream.common.appender.spout.processor.*;

/**
 * Created by ximeiwang on 2017/12/19.
 */
public class MongoMessageHandler extends AbstractMessageHandler {

    private RecordProcessor<String, byte[]> controlProcessor;
    private RecordProcessor<String, byte[]> initialLoadProcessor;
    private RecordProcessor<String, byte[]> defaultProcessor;

    public MongoMessageHandler(RecordProcessListener rpListener, ConsumerListener consumerListener) {
        super(rpListener, consumerListener);
        this.createProcessors();
        initialLoadTableNs = initialLoadTableNs.toLowerCase();
    }

    private void createProcessors() {
        this.controlProcessor = new CtrlEventProcessor(listener, consumerListener);
        this.initialLoadProcessor = new MongoInitialLoadProcessor(controlTopics, listener, consumerListener);
        this.defaultProcessor = new MongoDefaultProcessor(listener, consumerListener);
    }

    @Override
    protected RecordProcessor<String, byte[]> chooseProcessor(String recordKey, String topic) {
        if (recordKey != null && recordKey.startsWith(initialLoadTableNs)) {
            return initialLoadProcessor;
        }
        if (controlTopics.contains(topic)) {
            return controlProcessor;
        }
        return defaultProcessor;
    }
}
