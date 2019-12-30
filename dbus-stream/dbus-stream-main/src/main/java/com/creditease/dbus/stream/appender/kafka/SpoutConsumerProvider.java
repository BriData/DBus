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


package com.creditease.dbus.stream.appender.kafka;

import avro.shaded.com.google.common.collect.Lists;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.appender.utils.ZKNodeOperator;
import com.creditease.dbus.stream.common.Constants;
import com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Shrimp on 16/7/20.
 */
public class SpoutConsumerProvider implements ConsumerProvider<String, byte[]> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private ZKNodeOperator offsetOperator;

    public SpoutConsumerProvider(ZKNodeOperator operator) {
        this.offsetOperator = operator;
    }

    @Override
    public Consumer<String, byte[]> consumer(Properties properties, List<String> topics) {

        try {
            Properties props = PropertiesHolder.getProperties(Constants.Properties.CONSUMER_CONFIG);
            Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);

            List<TopicPartition> tpList = topics.stream().map(topic -> new TopicPartition(topic, 0))
                    .collect(Collectors.toList());
            consumer.assign(tpList);

            logger.info("KafkaConsumer has bean created {topics:[{}]}", Joiner.on(",").join(topics));
            return consumer;
        } catch (Exception e) {
            logger.error("Creating consumer error!!!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pause(Consumer<String, byte[]> consumer, Collection<TopicInfo> pausedTopics) {
        try {
            List<TopicPartition> parsedTopics = pausedTopics.stream()
                    .map(topic -> topic.partition())
                    .collect(Collectors.toList());

            Map<String, Object> map = offsetOperator.getData();
            if (map != null && !map.isEmpty()) {
                Set<String> topics = consumer.assignment().stream()
                        .map(TopicPartition::topic).collect(Collectors.toSet());
                for (Map.Entry<String, Object> e : map.entrySet()) {
                    if (!topics.contains(e.getKey()) || Constants.NONE.equals(e.getValue())) {
                        continue;
                    }

                    if (Constants.END.equalsIgnoreCase(e.getValue().toString())) {
                        consumer.seekToEnd(Lists.newArrayList(new TopicPartition(e.getKey(), 0)));
                    } else if (Constants.BEGIN.equalsIgnoreCase(e.getValue().toString())) {
                        consumer.seekToBeginning(Lists.newArrayList(new TopicPartition(e.getKey(), 0)));
                    } else {
                        try {
                            long offset = Long.parseLong(e.getValue().toString());
                            consumer.seek(new TopicPartition(e.getKey(), 0), offset);
                        } catch (NumberFormatException e1) {
                            logger.warn("Seek partition:{} to offset[{}] error", e.getKey(), e.getValue());
                        }
                    }
                }

                offsetOperator.setData(Collections.emptyMap(), false);
            }
            consumer.pause(parsedTopics);
        } catch (Exception e) {
            logger.error("Pausing consumer error!!!", e);
            throw new RuntimeException(e);
        }
    }
}
