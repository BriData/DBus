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


package com.creditease.dbus.stream.common.appender.spout.processor;

import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.enums.Command;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Created by Shrimp on 16/6/21.
 */
public interface RecordProcessListener {

    String getListenerId();

    /**
     * 标识重新加载数据
     *
     * @param record kafka consumer record
     */
    void markReloading(DBusConsumerRecord<String, byte[]> record, Map<String, Object> params);

    void emitData(EmitData data, Command cmd, Object msgId);

    Future<RecordMetadata> sendRecord(ProducerRecord<String, byte[]> producerRecord);

    void reduceFlowSize(int recordSize);

    void increaseFlowSize(int recordSize);
}
