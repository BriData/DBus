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

package com.creditease.dbus.heartbeat.event.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.dao.impl.LoadDbusConfigDaoImpl;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.HTMLUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.ProjectNotifyEmailsVO;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


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
            // 获取心跳配置信息
            HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
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
                            IMail mail = DBusMailFactory.build();
                            ControlMessage controlMessage = JSON.parseObject(value, ControlMessage.class);
                            Map<String, Object> payload = controlMessage.getPayload();

                            String datasource = payload.get("datasource") == null ? "" : payload.get("datasource").toString();
                            String schema = payload.get("schema") == null ? "" : payload.get("schema").toString();
                            String table = payload.get("table") == null ? "" : payload.get("table").toString();

                            JSONObject jsonObjectvalue = JSONObject.parseObject(value);
                            String compareResult = HTMLUtil.globalControlEmailJsonVo2HTML(jsonObjectvalue);
                            String compatibleText = HTMLUtil.isVersionChangeCompatible(jsonObjectvalue.getJSONObject("payload"));
                            String subject = "DBus表结构"+compatibleText+"变更通知 ";
                            String contents = MsgUtil.format(Constants.MAIL_SCHEMA_CHANGE,
                                    "表结构"+compatibleText+"变更通知", datasource, schema, table,
                                    DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                                    IMail.ENV, compareResult);



                            String email = getSchemaChangeEmail(datasource + "/" + schema);
                            String projectRelatedEmail = getProjectRelatedSchemaChangeEmail(datasource, schema, table);
                            if (StringUtils.isNotBlank(projectRelatedEmail)) {
                                email += "," + projectRelatedEmail;
                            }
                            if (StringUtils.isBlank(email)) {
                                LoggerFactory.getLogger().error("[Global-Control-kafka-Consumer-event] email setting is empty!");
                                continue;
                            }
                            LOG.info("[Global-Control-kafka-Consumer-event] 表结构变更通知邮件报警收件人EMail地址:{}.", email);

                            Message msg = new Message();
                            msg.setAddress(email);
                            msg.setContents(contents);
                            msg.setSubject(subject);

                            msg.setHost(hbConf.getAlarmMailSMTPAddress());
                            if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                                msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                            msg.setUserName(hbConf.getAlarmMailUser());
                            msg.setPassword(hbConf.getAlarmMailPass());
                            msg.setFromAddress(hbConf.getAlarmSendEmail());

                            boolean ret = mail.send(msg);
                            LOG.info("[Global-Control-kafka-Consumer-event] 发送邮件", ret ? "成功" : "失败");

                        } else if (ControlType.COMMON_EMAIL_MESSAGE.toString().equals(key)) {
                            IMail mail = DBusMailFactory.build();
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

                            msg.setHost(hbConf.getAlarmMailSMTPAddress());
                            if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                                msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                            msg.setUserName(hbConf.getAlarmMailUser());
                            msg.setPassword(hbConf.getAlarmMailPass());
                            msg.setFromAddress(hbConf.getAlarmSendEmail());

                            boolean ret = mail.send(msg);
                            LOG.info("[Global-Control-kafka-Consumer-event] 发送邮件", ret == true ? "成功" : "失败");
                        }else  if (ControlType.KEEPER_PROJECT_EXPIRE.toString().equals(key)) {
                            ControlMessage controlMessage = JSON.parseObject(value, ControlMessage.class);
                            Map<String, Object> payload = controlMessage.getPayload();

                            LOG.info("[Global-Control-kafka-Consumer-event] 收到项目到期消息");
                            IMail mail = DBusMailFactory.build();
                            String projectName = payload.get("project_name") == null ? "" : payload.get("project_name").toString();
                            List<String> emails = payload.get("emails") == null ? null : (List<String>) payload.get("emails");
                            String expireTime = payload.get("expire_time") == null ? "" : payload.get("expire_time").toString();
                            String remainTime = payload.get("remain_time") == null ? "" : payload.get("remain_time").toString();
                            String offlineTime = payload.get("offline_time") == null ? "" : payload.get("offline_time").toString();
                            if(emails==null || emails.size()==0){
                                LOG.warn("[Global-Control-kafka-Consumer-event] 项目到期告警：邮箱地址有误");
                                continue;
                            }
                            LOG.info("[Global-Control-kafka-Consumer-event] 收到项目到期消息，emails:{}",emails);
                            //"项目临到期"还是"项目下线"
                            String type = payload.get("type") == null ? "" : payload.get("type").toString();
                            String warn = StringUtils.equals(type,"项目下线") ? "该项目已于"+offlineTime+"下线" :
                                    "项目有效期余额不足";

                            String subject = "DBus-Keeper"+type+"告警";
                            String content = MsgUtil.format(Constants.PROJECT_EXPIRE_TIME,type,projectName,expireTime,
                                    remainTime,IMail.ENV,warn);

                            String email = StringUtils.join(emails,",");
                            Message msg = new Message();
                            msg.setAddress(email);
                            msg.setContents(content);
                            msg.setSubject(subject);

                            msg.setHost(hbConf.getAlarmMailSMTPAddress());
                            if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                                msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                            msg.setUserName(hbConf.getAlarmMailUser());
                            msg.setPassword(hbConf.getAlarmMailPass());
                            msg.setFromAddress(hbConf.getAlarmSendEmail());

                            LOG.info("[Global-Control-kafka-Consumer-event] 发送邮件，emails:{}",emails);
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

    public String getSchemaChangeEmail(String dsSchemaName) {
        String email = StringUtils.EMPTY;

        HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
        String isUse = hbConf.getSchemaChangeUseEmail();
        if (StringUtils.equals(isUse.toUpperCase(), "Y")) {
            email = hbConf.getSchemaChangeEmail();
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

    private String getProjectRelatedSchemaChangeEmail(String datasource, String schema, String table) {
        ILoadDbusConfigDao dao = new LoadDbusConfigDaoImpl();
        List<ProjectNotifyEmailsVO> emailsVOs = dao.queryRelatedNotifyEmails(Constants.CONFIG_DB_KEY, datasource, schema, table);
        Set<String> emails = new HashSet<>();
        for (ProjectNotifyEmailsVO emailsVO : emailsVOs) {
            if (emailsVO.getSchemaChangeEmails() != null) {
                Collections.addAll(emails, emailsVO.getSchemaChangeEmails());
            }
        }
        return StringUtils.join(emails, ",");
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

