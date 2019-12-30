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


package com.creditease.dbus.commons;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class DbusHelper {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static Map<Properties, Producer> producerMap = new HashMap();

    public static Producer getProducer(Properties props) throws Exception {
        if (producerMap.containsKey(props)) {
            return producerMap.get(props);
        } else {
            Producer producer = new KafkaProducer<>(props);
            producerMap.put(props, producer);
            return producer;
        }
    }

    public static Consumer<String, byte[]> createConsumer(Properties props, String subscribeTopic) throws Exception {
        TopicPartition topicPartition = new TopicPartition(subscribeTopic, 0);
        List<TopicPartition> topicPartitions = Arrays.asList(topicPartition);
        Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        // consumer.subscribe(Arrays.asList(subscribeTopics.split(",")));
        consumer.assign(topicPartitions);
        // consumer都是在topo启动时创建。当Topo重启，目前的策略是对于kafka中未处理的msg放弃。不再消费。所以seek to end。
        consumer.seekToEnd(topicPartitions);
        return consumer;
    }
}
