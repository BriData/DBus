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

package com.creditease.dbus.heartbeat.event.impl;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.HTMLUtil;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;


public class GlobalControlKafkaConsumerEvent extends KafkaConsumerEvent {
    private int waitingTimes = 0;


    public GlobalControlKafkaConsumerEvent(String topic) {
        super(topic);
        /*this.topic = topic;
        Properties props = HeartBeatConfigContainer.getInstance().getKafkaConsumerConfig();
        try {
            dataConsumer = new KafkaConsumer<>(props);
            partition0 = new TopicPartition(this.topic, 0);
            dataConsumer.assign(Arrays.asList(partition0));
            dataConsumer.seekToEnd(Arrays.asList(partition0));
            KafkaConsumerContainer.getInstances().putConsumer(this.topic, dataConsumer);


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }


    @Override
    public void run() {
        String key = null;
        String value = null;
        try {
            while (isRun.get()) {
                try {
                    ConsumerRecords<String, String> records = dataConsumer.poll(1000);
                    if (records.isEmpty()) {
                        waitingTimes++;
                        if (waitingTimes >= 60) {
                            waitingTimes = 0;
                            LoggerFactory.getLogger().info("[Global-Control-kafka-Consumer-event]  polling ....");
                        }
                        continue;
                    }
                    waitingTimes = 0;

                    for (ConsumerRecord<String, String> record : records) {
                        key = record.key();
                        value = record.value();

                        LoggerFactory.getLogger().info("[Global-Control-kafka-Consumer-event]  topic:{}, key:{}, value:{}", topic, key, value);
                        if (StringUtils.isEmpty(value)) {
                            LoggerFactory.getLogger().error("[Global-Control-kafka-Consumer-event]  topic:{}, value:null, offset:{}", topic, record.offset());
                            continue;
                        }

                        if (ControlType.G_META_SYNC_WARNING.toString().equals(key)) {
                            JSONObject jsonValue = JSONObject.parseObject(value);
                            String subject = "表结构变更通知";
                            String contents = HTMLUtil.globalControlEmailJsonVo2HTML(jsonValue);

                            //TODO 需要填一下 扩展的邮件schema
                            String email = getEmail(null);
                            if (StringUtils.isBlank(email)) {
                                LoggerFactory.getLogger().error("[Global-Control-kafka-Consumer-event] email setting is empty!");
                                continue;
                            }
                            LOG.info("[Global-Control-kafka-Consumer-event] 表结构变更通知邮件报警收件人EMail地址:{}.", email);

                            Message msg = new Message();
                            msg.setAddress(email);
                            msg.setContents(contents);
                            msg.setSubject(subject);
                            IMail mail = DBusMailFactory.build();
                            boolean ret = mail.send(msg);
                            LOG.info("[Global-Control-kafka-Consumer-event] 发送邮件", ret == true ? "成功" : "失败");

                        } else if (ControlType.COMMON_EMAIL_MESSAGE.toString().equals(key)) {

                            JSONObject jsonValue = JSONObject.parseObject(value);

                            JSONObject payload = jsonValue.getJSONObject("payload");
                            String dsSchema = payload.getString("datasource_schema");
                            String subject = payload.getString("subject");
                            String contents = payload.getString("contents");

                            String email = getEmail(dsSchema);
                            if (StringUtils.isBlank(email)) {
                                LoggerFactory.getLogger().error("[Global-Control-kafka-Consumer-event] email setting is empty!");
                                continue;
                            }
                            LOG.info("[Global-Control-kafka-Consumer-event] 通用邮件消息 EMail地址:{}.", email);

                            Message msg = new Message();
                            msg.setAddress(email);
                            msg.setContents(contents);
                            msg.setSubject(subject);
                            IMail mail = DBusMailFactory.build();
                            boolean ret = mail.send(msg);
                            LOG.info("[Global-Control-kafka-Consumer-event] 发送邮件", ret == true ? "成功" : "失败");
                        }
                    }
                    Thread.sleep(1000);
                } catch (Exception e) {
                    LOG.error("[Global-Control-kafka-Consumer-event] topic: " + topic + " ,value:" + value, e);
                    //stop();
                }
            }
        } catch (Exception e) {
            LOG.error("[Global-Control-kafka-Consumer-event] topic: " + topic + " ,value:" + value, e);
        } finally {
            if (dataConsumer != null) {
                dataConsumer.commitSync();
                dataConsumer.close();
                dataConsumer = null;
            }
            if (statProducer != null) {
                statProducer.flush();
                statProducer.close();
                statProducer = null;
            }
            LOG.info("[Global-Control-kafka-Consumer-event] stop. topic: " + topic + ",t:" + Thread.currentThread().getName());
        }
    }

    public String getEmail(String dsSchemaName) {
        String email = StringUtils.EMPTY;

        HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
        String adminUseEmail = hbConf.getAdminUseEmail();
        if (StringUtils.equals(adminUseEmail.toUpperCase(), "Y")) {
            email = hbConf.getAdminEmail();
        }

        if (StringUtils.isNotBlank(dsSchemaName)) {
            Map<String, Map<String, String>> additionalNotify = hbConf.getAdditionalNotify();
            Map<String, String> map = additionalNotify.get(dsSchemaName);
            if (map != null && StringUtils.equals(map.get("UseEmail").toUpperCase(), "Y")) {
                if (StringUtils.isNotBlank(email)) {
                    email = StringUtils.join(new String[]{email, map.get("Email")}, ",");
                } else {
                    email = map.get("Email");
                }
            }
        }

        return email;
    }

//    public static void main(String[] args) {
//        String topic="global_ctrl_topic";
//
//        IResource<Properties> resource = new KafkaFileConfigResource("consumer.properties");
//        Properties consumer = resource.load();
//        HeartBeatConfigContainer.getInstance().setKafkaConsumerConfig(consumer);
//        Properties props = HeartBeatConfigContainer.getInstance().getKafkaConsumerConfig();
//        try {
//            KafkaConsumer<String ,String > dataConsumer = new KafkaConsumer<>(props);
//            TopicPartition partition0 = new TopicPartition(topic, 0);
//            dataConsumer.assign(Arrays.asList(partition0));
//            dataConsumer.seekToEnd(Arrays.asList(partition0));
//
//            while(true)
//            {
//                ConsumerRecords records = dataConsumer.poll(1000);
//                System.out.println(records);
//            }
//
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//    }
}

