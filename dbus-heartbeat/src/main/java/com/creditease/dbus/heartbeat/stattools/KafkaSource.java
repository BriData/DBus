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

package com.creditease.dbus.heartbeat.stattools;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.ConfUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.bind.PropertyException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

/**
 * Created by dongwang47 on 2016/9/2.
 */
public class KafkaSource {

    private Logger LOG = LoggerFactory.getLogger();

    private final static String CONFIG_PROPERTIES = "stat_config.properties";
    private final static String CONSUMER_PROPERTIES = "consumer.properties";

    private String statTopic = null;
    private TopicPartition statTopicPartition = null;
    private KafkaConsumer<String, String> consumer = null;

    private int count = 0;


    public KafkaSource () throws IOException, PropertyException  {
        Properties configs = ConfUtils.getProps(CONFIG_PROPERTIES);
        statTopic = configs.getProperty(Constants.STATISTIC_TOPIC);
        if (statTopic == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.STATISTIC_TOPIC);
        }

        statTopicPartition = new TopicPartition(statTopic, 0);

        Properties statProps = ConfUtils.getProps(CONSUMER_PROPERTIES);
        statProps.setProperty("enable.auto.commit", "false");
        List<TopicPartition> topics = Arrays.asList(statTopicPartition);
        consumer = new KafkaConsumer(statProps);
        consumer.assign(topics);

        long beforeOffset = consumer.position(statTopicPartition);
        String offset = configs.getProperty("kafka.offset");
        if (offset.equalsIgnoreCase("none")) {
            ; // do nothing
        } else if  (offset.equalsIgnoreCase("begin")) {
            consumer.seekToBeginning(Lists.newArrayList(statTopicPartition));
        } else if (offset.equalsIgnoreCase("end")) {
            consumer.seekToEnd(Lists.newArrayList(statTopicPartition));
        } else {
            long nOffset = Long.parseLong(offset);
            consumer.seek(statTopicPartition, nOffset);
        }
        long afferOffset = consumer.position(statTopicPartition);
        LOG.info(String.format("init kafkaSoure OK. beforeOffset %d, afferOffset=%d", beforeOffset, afferOffset));
    }


    public List<StatMessage> poll() {
                    /* 快速取，如果没有就立刻返回 */
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (records.count() == 0) {
            count++;
            if (count % 60 == 0) {
                count = 0;
                LOG.info(String.format("running on %s (offset=%d).......", statTopic,  consumer.position(statTopicPartition)));
            }
            return null;
        }

        LOG.info(String.format("KafkaSource got %d records......", records.count()));

        List<StatMessage> list = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            long offset = record.offset();

            StatMessage msg = StatMessage.parse(record.value());
            list.add(msg);
            //logger.info(String.format("KafkaSource got record key=%s, offset=%d......", key, offset));
        }

        return list;
    }

    public void commitOffset() {
        long offset = consumer.position(statTopicPartition);
        LOG.info(String.format("commit offset %d", offset));

        commitOffset(offset);
    }

    public void commitOffset(long offset) {
        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(offset + 1, "");

        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(statTopicPartition, offsetMeta);

        OffsetCommitCallback callback = new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                if (e != null) {
                    LOG.warn(String.format("CommitAsync failed!!!! offset %d, Topic %s", offset, statTopic));
                }
                else {
                    ; //do nothing when OK;
                    //logger.info(String.format("OK. offset %d, Topic %s", record.offset(), record.topic()));
                }
            }
        };
        consumer.commitAsync(offsetMap, callback);
    }


    public void cleanUp() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }
}
