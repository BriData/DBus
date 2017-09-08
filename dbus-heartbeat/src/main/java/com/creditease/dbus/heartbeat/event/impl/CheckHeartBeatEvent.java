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

import com.creditease.dbus.components.sms.DBusSmsFactory;
import com.creditease.dbus.components.sms.ISms;
import com.creditease.dbus.components.sms.SmsMessage;
import com.creditease.dbus.components.sms.SmsType;
import com.creditease.dbus.heartbeat.container.AlarmResultContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.event.AlarmType;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.CheckVo;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.PacketVo;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang.StringUtils;

/**
 * 用于检测每个schema heartbeat数据包是否有超时。
 * heartbeat packet eg:{"node":"/dbus/heartbeat/monitor/TEST","time":1466567943377}
 * 用当前系统时间(currentTimeMillis)减去packet.time值,大于heartBeatTimeout(znode:/dbus/heartbeat/config)报警
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class CheckHeartBeatEvent extends AbstractEvent {

    public CheckHeartBeatEvent(long interval, CountDownLatch cdl) {
        super(interval, cdl);
    }

    @Override
    public void fire(DsVo ds, MonitorNodeVo node, String path, long txTime) {
        try {
            // 反序列化packet信息
            PacketVo packet = deserialize(path, PacketVo.class);
            if (packet == null)
                return;

            CheckVo check = AlarmResultContainer.getInstance().get(path);
            if (check == null) {
                check = new CheckVo();
                check.setType(AlarmType.HEART_BEAT);
                check.setPath(path);
                AlarmResultContainer.getInstance().put(path, check);
            }

            // 第一次发送为heartbeat发送时间,之后为接收心跳数据的时间
            long time = packet.getTime();
            // 比较时用当前时间
            long currentTime = System.currentTimeMillis();
            // 获取心跳配置信息
            HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
            // 心跳超时时间(单位：毫秒)
            long heartBeatTimeout = hbConf.getHeartBeatTimeout();
            // 报警超时时间
            long alarmTtl = hbConf.getAlarmTtl();
            // 最大报警次数
            int maxAlarmCnt = hbConf.getMaxAlarmCnt();
            // 修正值(不同服务器时间不同步时使用)
            long correcteValue = hbConf.getCorrecteValue();
            // 当前时间和接收到时间的差值
            long diffVal = currentTime - time + correcteValue;

                        // 根据不同的数据库决定是否需要修改heartBeatTimeout
            Map<String, Map<String, String>> heartBeatTimeoutAdditional = hbConf.getHeartBeatTimeoutAdditional();
            String databaseSchemaName = node.getDsName() + "/" + node.getSchema();


            if (heartBeatTimeoutAdditional != null) {
                for (Map.Entry<String, Map<String, String>> entry : heartBeatTimeoutAdditional.entrySet()) {
                    if (StringUtils.equals(entry.getKey(), databaseSchemaName)) {
                        String startTime = entry.getValue().get("startTime");
                        String endTime = entry.getValue().get("endTime");
                        if (DateUtil.isCurrentTimeInInterval(startTime, endTime)) {
                            heartBeatTimeout = Long.parseLong(entry.getValue().get("heartBeatTimeout"));
                            LOG.info("changed timeout, 已修改{}的超时时间为{}",entry.getKey(),heartBeatTimeout);
                        }
                    }
                }
            }

//            LOG.info("nznode:{},diffVal:{}, heartBeatTimeout:{}",
//                    new Object[]{path, String.valueOf(diffVal),  String.valueOf(heartBeatTimeout)});

            if (diffVal > heartBeatTimeout) {

                LOG.info("znode:{},修正值:{},当前时间:{},接收到时间{},差值:{} 秒.",
                        new Object[]{path, String.valueOf(correcteValue), String.valueOf(currentTime),
                                String.valueOf(time), String.valueOf((double) diffVal / 1000)});

                check.setNormal(false);

                //超过alarm Ttl 可以再报
                if (currentTime - check.getLastAlarmTime() > alarmTtl) {
                    check.setAlarmCnt(0);
                    check.setTimeoutCnt(check.getTimeoutCnt() + 1);
                }

                int alarmCnt = check.getAlarmCnt();
                if (alarmCnt < maxAlarmCnt) {
                    check.setAlarmCnt(alarmCnt + 1);
                    check.setLastAlarmTime(currentTime); // TODO: 2016/11/30  modify to long

                    // 邮件和短信报警
                    String key = StringUtils.substringBeforeLast(path, "/");
                    key = StringUtils.replace(key, "/DBus/HeartBeat/Monitor/", StringUtils.EMPTY);
                    String adminSMSNo = hbConf.getAdminSMSNo();
                    String adminUseSMS = hbConf.getAdminUseSMS();
                    String adminEmail = hbConf.getAdminEmail();
                    String adminUseEmail = hbConf.getAdminUseEmail();
                    Map<String, Map<String, String>> additionalNotify = hbConf.getAdditionalNotify();
                    Map<String, String> map = additionalNotify.get(key);

                    if (map == null) {
                        LOG.info("[check-event] 没有对key:{},个性化短信和邮件报警进行配置.", key);
                    } else {
                        LOG.debug("[check-event] key:{},的个性化邮件和短信配置{}", key, JsonUtil.toJson(map));
                    }

                    String telNos = StringUtils.EMPTY;
                    if (StringUtils.equals(adminUseSMS.toUpperCase(), "Y")) {
                        telNos = adminSMSNo;
                    }
                    if (map != null && StringUtils.equals(map.get("UseSMS").toUpperCase(), "Y")) {
                        if (StringUtils.isNotBlank(telNos)) {
                            telNos = StringUtils.join(new String[]{telNos, map.get("SMSNo")}, ",");
                        } else {
                            telNos = map.get("SMSNo");
                        }
                    }
                    if (StringUtils.isNotBlank(telNos)) {
                        String contents = MessageFormat.format(Constants.MAIL_HEART_BEAT,
                                path, check.getAlarmCnt(), check.getTimeoutCnt());
                        SmsMessage msg = new SmsMessage();
                        msg.setTelNo(telNos);
                        msg.setContents(contents);
                        msg.setSmsType(SmsType.HEART_BEAT.getName());
                        ISms sms = DBusSmsFactory.bulid();
                        sms.send(msg);
                    }

                    String email = StringUtils.EMPTY;
                    if (StringUtils.equals(adminUseEmail.toUpperCase(), "Y")) {
                        email = adminEmail;
                    }
                    if (map != null && StringUtils.equals(map.get("UseEmail").toUpperCase(), "Y")) {
                        if (StringUtils.isNotBlank(email)) {
                            email = StringUtils.join(new String[]{email, map.get("Email")}, ",");
                        } else {
                            email = map.get("Email");
                        }
                    }
                    if (StringUtils.isNotBlank(email)) {
                        LOG.info("[check-event] 接收心跳邮件报警收件人EMail地址:{}.", email);
                        String subject = "DBus心跳监控报警";
                        String contents = MsgUtil.format(Constants.MAIL_HEART_BEAT, path, check.getAlarmCnt(), check.getTimeoutCnt());
                        Message msg = new Message();
                        msg.setAddress(email);
                        msg.setContents(contents);
                        msg.setSubject(subject);
                        IMail mail = DBusMailFactory.build();
                        mail.send(msg);
                    }
                }
                LOG.warn(check.toString());
            } else {
                check.setNormal(true);
                check.setAlarmCnt(0);
                check.setTimeoutCnt(0);
                check.setLastAlarmTime(0);
            }

        } catch (Exception e) {
            LOG.error("[check-event]", e);
        }
    }

}
