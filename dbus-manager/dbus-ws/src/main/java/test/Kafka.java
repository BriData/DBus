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

package test;

import com.creditease.dbus.commons.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Kafka {
    public static void main(String[] args) throws IOException, InterruptedException{
        Properties properties = PropertiesUtils.getProps("consumer.properties");
        properties.setProperty("client.id","whtestconsumer");
        properties.setProperty("group.id","whtestconsumer");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //properties.setProperty("auto.offset.reset", "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topic = "uav-test.monitor.result";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> topics = Arrays.asList(topicPartition);
        consumer.assign(topics);
        consumer.seekToEnd(topics);
        long current = consumer.position(topicPartition);
        consumer.seek(topicPartition, current-1000);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            Thread.sleep(1);
        }
    }
}
