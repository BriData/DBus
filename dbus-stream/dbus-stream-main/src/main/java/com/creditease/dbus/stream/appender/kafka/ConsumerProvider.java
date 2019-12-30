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

import com.creditease.dbus.stream.appender.exception.MethodNotSupportedException;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer 工厂
 * Created by Shrimp on 16/7/20.
 */
public interface ConsumerProvider<K, V> {
    Consumer<K, V> consumer(Properties properties, List<String> topics);

    default void pause(Consumer<String, byte[]> consumer, Collection<TopicInfo> topics) {
        throw new MethodNotSupportedException("Please override method 'pause(List<String> list)' in your implementation");
    }
}
