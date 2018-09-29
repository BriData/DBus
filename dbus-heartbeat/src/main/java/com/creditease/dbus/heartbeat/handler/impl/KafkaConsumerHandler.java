/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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


package com.creditease.dbus.heartbeat.handler.impl;


import java.util.HashSet;
import java.util.Set;

import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.KafkaConsumerContainer;
import com.creditease.dbus.heartbeat.event.impl.GlobalControlKafkaConsumerEvent;
import com.creditease.dbus.heartbeat.event.impl.KafkaConsumerEvent;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;



public class KafkaConsumerHandler extends AbstractHandler {

    @Override
    public void process() {
        //获得所有的topic
        Set<TargetTopicVo> topics = HeartBeatConfigContainer.getInstance().getTargetTopic();
        Set<String> topicSet = new HashSet<String>();
        for(TargetTopicVo vo : topics){
            topicSet.add(vo.getTargetTopic());
        }
        LoggerFactory.getLogger().info("[kafka-consumer-handler] topic count:{}, {}", topicSet.size(), topicSet.toString());

        KafkaConsumerContainer container = KafkaConsumerContainer.getInstances();
        container.initThreadPool(topicSet.size());
        for (String topic : topicSet) {
            /*Thread t = new Thread(new KafkaConsumerEvent(key, stream));
            t.start();*/
            KafkaConsumerEvent event = new KafkaConsumerEvent(topic);
            container.submit(event);
        }
        // 单独添加一个全局的global_ctrl消费者
        KafkaConsumerEvent event = new GlobalControlKafkaConsumerEvent(Constants.GLOBAL_CTRL_TOPIC);
        container.submit(event);





    }



    /*@Override
    public void process() {
        KafkaConsumerContainer container = KafkaConsumerContainer.getInstance();
        container.initThreadPool();
        Set<String> topics = HeartBeatConfigContainer.getInstance().getTargetTopic();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        for (String topic : topics) {
            topicCountMap.put(topic, Integer.valueOf(1));
        }
        LoggerFactory.getLogger().info("[kafka-consumer-handler] {}", topicCountMap.toString());

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                container.getConsumer().createMessageStreams(topicCountMap);
        for (String key : consumerMap.keySet()) {
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(key);
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                Thread t = new Thread(new KafkaConsumerEvent(key, stream));
                t.start();
                container.submit(new KafkaConsumerEvent(key, stream));
            }
        }
    }
*/
}
