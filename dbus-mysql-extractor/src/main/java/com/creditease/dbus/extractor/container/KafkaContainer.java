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


package com.creditease.dbus.extractor.container;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class KafkaContainer {
    private static KafkaContainer kafkaContainer;

    private Map<Properties, Producer> producerMap = new HashMap<Properties, Producer>();

    private KafkaContainer() {

    }

    public static KafkaContainer getInstance() {
        if (kafkaContainer == null) {
            synchronized (KafkaContainer.class) {
                if (kafkaContainer == null)
                    kafkaContainer = new KafkaContainer();
            }
        }
        return kafkaContainer;
    }

    public Producer getProducer(Properties props) {
        if (producerMap.containsKey(props)) {
            return producerMap.get(props);
        } else {
            Producer producer = new KafkaProducer<>(props);
            producerMap.put(props, producer);
            return producer;
        }
    }

    public void clear() {
        producerMap.clear();
    }
}
