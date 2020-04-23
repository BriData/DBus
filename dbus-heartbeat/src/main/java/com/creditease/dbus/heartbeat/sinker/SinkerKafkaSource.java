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


package com.creditease.dbus.heartbeat.sinker;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.ConfUtils;
import com.creditease.dbus.heartbeat.util.KafkaUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import javax.xml.bind.PropertyException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class SinkerKafkaSource {

    private Logger LOG = LoggerFactory.getLogger();

    private final static String CONSUMER_PROPERTIES = "consumer.properties";
    private final static String CONFIG_PROPERTIES = "config.properties";

    private String topic = null;
    private TopicPartition topicPartition = null;
    private KafkaConsumer<String, String> consumer = null;

    private int count = 0;

    public SinkerKafkaSource() throws IOException, PropertyException {
        Properties config = ConfUtils.getProps(CONFIG_PROPERTIES);
        topic = config.getProperty(Constants.SINKER_HEARTBEAT_TOPIC);
        if (topic == null) {
            throw new PropertyException("[sinker] 配置参数文件内容不能为空！ " + Constants.SINKER_HEARTBEAT_TOPIC);
        }

        topicPartition = new TopicPartition(topic, 0);

        Properties statProps = ConfUtils.getProps(CONSUMER_PROPERTIES);
        statProps.setProperty("enable.auto.commit", "true");
        statProps.setProperty("client.id", "heartbeat_consumer_sinker_client");
        List<TopicPartition> topics = Arrays.asList(topicPartition);
        //security
        if (KafkaUtil.checkSecurity()) {
            statProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        }

        LOG.info("[sinker] SinkerKafkaSource message: set max.poll.records=1000");
        statProps.setProperty("max.poll.records", "1000");

        consumer = new KafkaConsumer(statProps);
        consumer.assign(topics);

        long beforeOffset = consumer.position(topicPartition);
        String offset = config.getProperty("sinker.kafka.offset");
        if (StringUtils.isBlank(offset) || offset.equalsIgnoreCase("none")) {
            // do nothing
        } else if (offset.equalsIgnoreCase("begin")) {
            consumer.seekToBeginning(Lists.newArrayList(topicPartition));
        } else if (offset.equalsIgnoreCase("end")) {
            consumer.seekToEnd(Lists.newArrayList(topicPartition));
        } else {
            long nOffset = Long.parseLong(offset);
            consumer.seek(topicPartition, nOffset);
        }
        long afferOffset = consumer.position(topicPartition);
        LOG.info("[sinker] SinkerKafkaSource init OK. beforeOffset {}, afferOffset={}", beforeOffset, afferOffset);
    }


    public List<String> poll() {
        /* 快速取，如果没有就立刻返回 */
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (records.count() == 0) {
            count++;
            if (count % 60 == 0) {
                count = 0;
                LOG.info("[sinker] SinkerKafkaSource running on {} (offset={}).......", topic, consumer.position(topicPartition));
            }
            return null;
        }

        List<String> list = new ArrayList<>();
        long maxOffset = 0l;
        for (ConsumerRecord<String, String> record : records) {
            if (record.offset() > maxOffset) maxOffset = record.offset();
            list.add(record.key());
        }

        LOG.info("[sinker] SinkerKafkaSource got {} records, max offset: {}", records.count(), maxOffset);
        return list;
    }

    public void cleanUp() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }
}
