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


package com.creditease.dbus.stream.dispatcher.tools;

import com.creditease.dbus.stream.common.DataSourceInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhenlinzhong on 2018/4/25.
 */
public class Db2OffsetReset implements OffsetResetProvider {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConsumer<byte[], byte[]> consumer = null;
    private DataSourceInfo dsInfo = null;

    public Db2OffsetReset(KafkaConsumer<byte[], byte[]> consumer, DataSourceInfo dsInfo) {
        this.consumer = consumer;
        this.dsInfo = dsInfo;
    }

    @Override
    public void offsetReset(Object... args) {
        List<TopicPartition> topicPartitions = (List<TopicPartition>) args[0];

        List<TopicPartition> topicPartition2Begin = new ArrayList<>();
        List<TopicPartition> topicPartition2End = new ArrayList<>();

        HashMap<String, String> topicAndOffset = new HashMap<>();
        String topicOffsets = dsInfo.getDataTopicOffset();

        if (StringUtils.equals(topicOffsets, "none")) return;

        String[] topicsArr = StringUtils.split(topicOffsets, ",");

        for (String topic : topicsArr) {
            topicAndOffset.put(StringUtils.split(topic, "->")[0], StringUtils.split(topic, "->")[1]);
        }

        for (TopicPartition tp : topicPartitions) {
            String topicName = tp.topic();
            if (StringUtils.contains(topicOffsets, topicName)) {
                String topicInfo = topicAndOffset.get(topicName);
                if (StringUtils.equals(topicInfo, "none")) {
                    break;
                } else if (StringUtils.equals(topicInfo, "begin")) {
                    topicPartition2Begin.add(tp);
                    consumer.seekToBeginning(topicPartition2Begin);
                    topicPartition2Begin.clear();
                    logger.info(String.format("TopicName : %s,  Offset changed to begin!", tp.topic()));
                } else if (StringUtils.equals(topicInfo, "end")) {
                    topicPartition2End.add(tp);
                    consumer.seekToEnd(topicPartition2End);
                    topicPartition2End.clear();
                    logger.info(String.format("TopicName : %s,  Offset changed to end!", tp.topic()));
                } else {
                    long nOffset = Long.parseLong(topicInfo);
                    consumer.seek(tp, nOffset);
                    logger.info(String.format("TopicName : %s,  Offset changed as: %d", tp.topic(), consumer.position(tp)));
                }
            }
        }

    }
}
