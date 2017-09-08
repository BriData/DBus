/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.heartbeat.util;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.log.LoggerFactory;

public class KafkaUtil {

    private KafkaUtil() {
    }

    public static boolean send(String msg, String topic, final String key) {
        boolean isOk = true;
        KafkaProducer<String, String> producer = null;
        try {
            Properties props = HeartBeatConfigContainer.getInstance().getKafkaProducerConfig();
            producer = new KafkaProducer<String, String>(props);
            producer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        LoggerFactory.getLogger().error("[kafka-send-error]", e);
                    else
                    {
                        LoggerFactory.getLogger().info(
                                String.format("信息发送到topic:%s,key:%s,offset:%s",
                                metadata.topic(), key, metadata.offset()));
                    }

                }
            });
        } catch (Exception e) {
            isOk = false;
            LoggerFactory.getLogger().error("[kafka-send-error]", e);
        } finally {
            if (producer != null)
                producer.close();
        }
        return isOk;
    }

}
