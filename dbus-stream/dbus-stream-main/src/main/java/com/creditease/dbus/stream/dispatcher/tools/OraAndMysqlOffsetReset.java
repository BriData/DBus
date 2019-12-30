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

import java.util.List;

/**
 * Created by zhenlinzhong on 2018/4/25.
 */
public class OraAndMysqlOffsetReset implements OffsetResetProvider {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaConsumer<String, byte[]> consumer = null;
    private DataSourceInfo dsInfo = null;

    public OraAndMysqlOffsetReset(KafkaConsumer<String, byte[]> consumer, DataSourceInfo dsInfo) {
        this.consumer = consumer;
        this.dsInfo = dsInfo;
    }

    @Override
    public void offsetReset(Object... args) {
        List<TopicPartition> topicPartitions = (List<TopicPartition>) args[0];
        String topicOffsets = dsInfo.getDataTopicOffset();

        for (TopicPartition tp : topicPartitions) {
            if (StringUtils.equals(topicOffsets, "none")) {
                break;
            } else if (StringUtils.equals(topicOffsets, "begin")) {
                consumer.seekToBeginning(topicPartitions);
                logger.info(String.format("Offset seek to begin, changed as: %d", consumer.position(tp)));
            } else if (StringUtils.equals(topicOffsets, "end")) {
                consumer.seekToEnd(topicPartitions);
                logger.info(String.format("Offset seek to end, changed as: %d", consumer.position(tp)));
            } else {
                long nOffset = Long.parseLong(topicOffsets);
                consumer.seek(tp, nOffset);
                logger.info(String.format("Offset changed as: %d", consumer.position(tp)));
            }
        }
    }
}
