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



package com.creditease.dbus.heartbeat.handler.impl;


import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.KafkaConsumerContainer;
import com.creditease.dbus.heartbeat.event.impl.GlobalControlKafkaConsumerEvent;
import com.creditease.dbus.heartbeat.event.impl.KafkaConsumerEvent;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class KafkaConsumerHandler extends AbstractHandler {

    @Override
    public void process() {
        //获得所有的topic
        Set<TargetTopicVo> topics = HeartBeatConfigContainer.getInstance().getTargetTopic();
        Set<String> topicSet = new HashSet<String>();
        Map<String, Map<String, Set<String>>> map = new HashMap<>();
        for (TargetTopicVo vo : topics) {
            topicSet.add(vo.getTargetTopic());
            if (map.containsKey(vo.getTargetTopic())) {
                if (map.get(vo.getTargetTopic()).containsKey(vo.getDsName())) {
                    map.get(vo.getTargetTopic()).get(vo.getDsName()).add(vo.getSchemaName());
                } else {
                    map.get(vo.getTargetTopic()).put(vo.getDsName(), new HashSet<>());
                    map.get(vo.getTargetTopic()).get(vo.getDsName()).add(vo.getSchemaName());
                }
            } else {
                map.put(vo.getTargetTopic(), new HashMap<>());
                if (map.get(vo.getTargetTopic()).containsKey(vo.getDsName())) {
                    map.get(vo.getTargetTopic()).get(vo.getDsName()).add(vo.getSchemaName());
                } else {
                    map.get(vo.getTargetTopic()).put(vo.getDsName(), new HashSet<>());
                    map.get(vo.getTargetTopic()).get(vo.getDsName()).add(vo.getSchemaName());
                }
            }
        }
        LoggerFactory.getLogger().info("[kafka-consumer-handler] topic count:{}, {}", topicSet.size(), topicSet.toString());
        LoggerFactory.getLogger().info("[kafka-consumer-handler] topic ds schema list:{}", JsonUtil.toJson(map));

        KafkaConsumerContainer container = KafkaConsumerContainer.getInstances();
        container.initThreadPool(topicSet.size());
        for (String topic : topicSet) {
            /*Thread t = new Thread(new KafkaConsumerEvent(key, stream));
            t.start();*/
            KafkaConsumerEvent event = new KafkaConsumerEvent(topic, map.get(topic));
            container.submit(event);
        }
        // 单独添加一个全局的global_ctrl消费者
        KafkaConsumerEvent event = new GlobalControlKafkaConsumerEvent(Constants.GLOBAL_CTRL_TOPIC);
        container.submit(event);


        //消费MAAS发送的Control Topic ,取得元数据信息并写入到MAAS指定topic
        /*String eventTopic = HeartBeatConfigContainer.getInstance().getmaasConf().getConfigProp().getProperty("maas.event.topic");
        String dataTopic = HeartBeatConfigContainer.getInstance().getmaasConf().getConfigProp().getProperty("maas.data.topic");
        IEvent maasEvent = new MaasEvent(eventTopic,dataTopic);
        Thread mtrEvent = new Thread(maasEvent, "maas-topic-rw-event");
        mtrEvent.start();
        EventContainer.getInstances().put(maasEvent, mtrEvent);


        // 元数据信息变更时，写入到MAAS指定topic
        IEvent maasAppenderEvent = new MaasAppenderEvent(Constants.GLOBAL_CTRL_TOPIC,dataTopic);
        Thread mapEvent = new Thread(maasAppenderEvent,"maas-appender-topic-rw-event");
        mapEvent.start();
        EventContainer.getInstances().put(maasAppenderEvent,mapEvent);*/


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
