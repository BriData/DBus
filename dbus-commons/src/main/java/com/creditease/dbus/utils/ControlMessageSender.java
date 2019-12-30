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


package com.creditease.dbus.utils;

import com.creditease.dbus.commons.ControlMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ControlMessageSender {

    private KafkaProducer<String, byte[]> producer = null;

    public ControlMessageSender(Properties properties) throws IOException {
        producer = new KafkaProducer<>(properties);
    }

    public long send(String topic, ControlMessage msg) throws Exception {
        String key = msg.getType();
        String jsonMessage = msg.toJSONString();
        byte[] message = jsonMessage.getBytes();

        try {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, key, message), null);
            RecordMetadata recordMetadata = result.get();
            return recordMetadata.offset();
        } finally {
            producer.close();
        }
    }
}
