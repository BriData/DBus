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


package com.creditease.dbus.heartbeat.event.impl;

import com.creditease.dbus.components.sms.DBusSmsFactory;
import com.creditease.dbus.components.sms.ISms;
import com.creditease.dbus.components.sms.SmsMessage;
import com.creditease.dbus.components.sms.SmsType;
import com.creditease.dbus.heartbeat.container.AlarmResultContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.dao.impl.LoadDbusConfigDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.event.AlarmType;
import com.creditease.dbus.heartbeat.parsing.FlowLineCheckParser;
import com.creditease.dbus.heartbeat.parsing.FlowLineCheckResult;
import com.creditease.dbus.heartbeat.parsing.RespParser;
import com.creditease.dbus.heartbeat.util.*;
import com.creditease.dbus.heartbeat.vo.*;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 用于检测每个schema heartbeat数据包是否有超时。
 * heartbeat packet eg:{"node":"/dbus/heartbeat/monitor/TEST","time":1466567943377}
 * 用当前系统时间(currentTimeMillis)减去packet.time值,大于heartBeatTimeout(znode:/dbus/heartbeat/config)报警
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class CheckHeartBeatEvent extends AbstractEvent {

    private StringBuilder html = null;

    private String emailAddress = StringUtils.EMPTY;

    private String dsNameWk = StringUtils.EMPTY;

    private String schemaNameWk = StringUtils.EMPTY;

    private String timeout = null;

    private String priority = null;

    public CheckHeartBeatEvent(long interval, CountDownLatch cdl) {
        super(interval, cdl);
        html = new StringBuilder();
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
                            LOG.info("changed timeout, 已修改{}的超时时间为{}", entry.getKey(), heartBeatTimeout);
                        }
                    }
                }
            }

//            LOG.info("nznode:{},diffVal:{}, heartBeatTimeout:{}",
//                    new Object[]{path, String.valueOf(diffVal),  String.valueOf(heartBeatTimeout)});

            /*if ((!StringUtils.equals(dsNameWk, node.getDsName()) ||
                    !StringUtils.equals(schemaNameWk, node.getSchema())) &&
                    StringUtils.isNotBlank(dsNameWk) &&
                    StringUtils.isNotBlank(schemaNameWk)) {

                long masterSlaveDelayTimeout = HeartBeatConfigContainer.getInstance().getHbConf().getMasterSlaveDelayTimeout();
                String masterSlaveDealyPath = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                masterSlaveDealyPath = StringUtils.join(new String[] {masterSlaveDealyPath, dsNameWk, schemaNameWk}, "/");
                MasterSlaveDelayVo msdVo = deserialize(masterSlaveDealyPath, MasterSlaveDelayVo.class);
                long delayTime = 0l;
                long synTime = 0l;
                if (msdVo != null) {
                    delayTime = msdVo.getDiff() == null ? 0l : msdVo.getDiff();
                    synTime = msdVo.getSynTime() == null ? 0l : msdVo.getSynTime();
                }

                LOG.info("[check-event] znode:{}, 主备延时:{}, 同步上时间:{}, eamail:{}.", masterSlaveDealyPath, delayTime, synTime, emailAddress);

                if (StringUtils.isNotBlank(emailAddress)) {
                    if (delayTime > masterSlaveDelayTimeout) {
                        IMail mail = DBusMailFactory.build();
                        String subject = "DBus主备不同步报警 ";
                        String contents = MsgUtil.format(Constants.MAIL_MASTER_SLAVE_DELAY,
                                "主备不同步报警", dsNameWk, schemaNameWk,
                                msdVo.getStrDiff(), msdVo.getMasterLatestTime(), msdVo.getSlaveLatestTime(),
                                IMail.ENV, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));

                        Message msg = new Message();
                        String projectRelatedEmail = getProjectRelatedSlaveDelayEmail(node.getDsName(), node.getSchema(), node.getTableName());
                        if (StringUtils.isNotBlank(projectRelatedEmail)) {
                            emailAddress += "," + projectRelatedEmail;
                        }
                        msg.setAddress(emailAddress);
                        msg.setContents(contents);
                        msg.setSubject(subject);

                        msg.setHost(hbConf.getAlarmMailSMTPAddress());
                        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                        msg.setUserName(hbConf.getAlarmMailUser());
                        msg.setPassword(hbConf.getAlarmMailPass());
                        msg.setFromAddress(hbConf.getAlarmSendEmail());

                        mail.send(msg);
                        html.delete(0, html.length());
                        emailAddress = "";
                    } else {
                        // 为了防止主备库刚刚追上产生的延时报警, 给程序10分钟的一个追数据的时间
                        // 如果10分钟还有追上数据就报警
                        if (html.length() > 0 && ((currentTime - synTime) > 1000 * 60 * 10)) {
                            IMail mail = DBusMailFactory.build();
                            String subject = "DBus超时报警 ";
                            String contents = MsgUtil.format(Constants.MAIL_HEART_BEAT_NEW,
                                    "超时报警", dsNameWk, schemaNameWk,
                                    DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                                    IMail.ENV,
                                    MsgUtil.format(AlarmResultContainer.getInstance().html(), html.toString()));
                            Message msg = new Message();
                            String projectRelatedEmail = getProjectRelatedTopologyDelayEmail(node.getDsName(), node.getSchema(), node.getTableName());
                            if (StringUtils.isNotBlank(projectRelatedEmail)) {
                                emailAddress += "," + projectRelatedEmail;
                            }
                            msg.setAddress(emailAddress);
                            msg.setContents(contents);
                            msg.setSubject(subject);

                            msg.setHost(hbConf.getAlarmMailSMTPAddress());
                            if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                                msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                            msg.setUserName(hbConf.getAlarmMailUser());
                            msg.setPassword(hbConf.getAlarmMailPass());
                            msg.setFromAddress(hbConf.getAlarmSendEmail());

                            mail.send(msg);
                            html.delete(0, html.length());
                            emailAddress = "";
                        }
                    }
                }
            }*/

            if ((!StringUtils.equals(dsNameWk, node.getDsName()) ||
                    !StringUtils.equals(schemaNameWk, node.getSchema())) &&
                    StringUtils.isNotBlank(dsNameWk) &&
                    StringUtils.isNotBlank(schemaNameWk)) {

                if (StringUtils.isNotBlank(emailAddress)) {
                    CommonConfigVo conmmonConfig = HeartBeatConfigContainer.getInstance().getConmmonConfig();

                    String url = MsgUtil.format(conmmonConfig.getFlowLineCheckUrl(), dsNameWk, schemaNameWk);
                    LOG.info("[check-event] 线路检查url: {}", url);
                    String resp = HttpUtil.get(url);
                    LOG.info("[check-event] 线路检查结果: {}", resp);
                    RespParser<FlowLineCheckResult> parser = new FlowLineCheckParser(resp);
                    if (parser.isSuccess()) {
                        long masterSlaveDelayTimeout = HeartBeatConfigContainer.getInstance().getHbConf().getMasterSlaveDelayTimeout();
                        FlowLineCheckResult result = parser.parse();
                        if (result.getDiffTime() > masterSlaveDelayTimeout) {
                            sendMasterSlaveTimeoutMail(node, hbConf, result);
                        } else {
                            String masterSlaveDealyPath = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                            masterSlaveDealyPath = StringUtils.join(new String[]{masterSlaveDealyPath, dsNameWk, schemaNameWk}, "/");
                            MasterSlaveDelayVo msdVo = deserialize(masterSlaveDealyPath, MasterSlaveDelayVo.class);
                            LOG.info("[check-event] 数据源:{}, 数据库:{}, 主备延时情况:{}.", dsNameWk, schemaNameWk, JsonUtil.toJson(msdVo));
                            long synTime = 0l;
                            if (msdVo != null) {
                                synTime = msdVo.getSynTime() == null ? 0l : msdVo.getSynTime();
                            }
                            // 为了防止主备库刚刚追上产生的延时报警, 给程序10分钟的一个追数据的时间
                            // 如果10分钟还有追上数据就报警
                            if (html.length() > 0 && ((currentTime - synTime) > 1000 * 60 * 10)) {
                                sendTimeoutMail(node, hbConf, result.toHtml());
                            }
                        }
                    } else {
                        sendTimeoutMail(node, hbConf, parser.toHtml());
                    }
                }

            }

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
                        // 旧的报警方式为以表为单位,每张表超时都要发邮件报警
                        /*String subject = "DBus心跳监控报警";
                        String contents = MsgUtil.format(Constants.MAIL_HEART_BEAT, path, check.getAlarmCnt(), check.getTimeoutCnt());
                        Message msg = new Message();
                        msg.setAddress(email);
                        msg.setContents(contents);
                        msg.setSubject(subject);
                        IMail mail = DBusMailFactory.build();
                        mail.send(msg);*/
                        html.append(check.html(DateUtil.diffDate(diffVal)));
                        emailAddress = email;
                        timeout = StringUtils.isNotBlank(timeout) ? DateUtil.diffHours(diffVal) : "0";
                        priority = diffVal > 2700000 ? "1" : "3";
                    }
                }
                LOG.warn(check.toString());
            } else {
                check.setNormal(true);
                check.setAlarmCnt(0);
                check.setTimeoutCnt(0);
                check.setLastAlarmTime(0);
            }

            dsNameWk = node.getDsName();
            schemaNameWk = node.getSchema();

        } catch (Exception e) {
            LOG.error("[check-event]", e);
        }
    }

    private void sendMasterSlaveTimeoutMail(MonitorNodeVo node, HeartBeatVo hbConf, FlowLineCheckResult result) {
        IMail mail = DBusMailFactory.build();
        String subject = String.format("%s-%s.%s不同步", result.getDiffDate(), dsNameWk, schemaNameWk);
        String contents = MsgUtil.format(Constants.MAIL_MASTER_SLAVE_DELAY,
                "主备不同步报警", dsNameWk, schemaNameWk,
                result.getDiffDate(), result.getMasterTime(), result.getSlaveTime(),
                IMail.ENV, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));

        Message msg = new Message();
        String projectRelatedEmail = getProjectRelatedSlaveDelayEmail(node.getDsName(), node.getSchema(), node.getTableName());
        if (StringUtils.isNotBlank(projectRelatedEmail)) {
            emailAddress += "," + projectRelatedEmail;
        }
        msg.setAddress(emailAddress);
        msg.setContents(contents);
        msg.setSubject(subject);
        // 这里只要超过1小时就标记为重要
        String priority = result.getDiffDate().contains("小时") || result.getDiffDate().contains("天") ? "1" : "3";
        msg.setPriority(priority);

        msg.setHost(hbConf.getAlarmMailSMTPAddress());
        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
        msg.setUserName(hbConf.getAlarmMailUser());
        msg.setPassword(hbConf.getAlarmMailPass());
        msg.setFromAddress(hbConf.getAlarmSendEmail());

        mail.send(msg);
        html.delete(0, html.length());
        emailAddress = "";
    }

    private void sendTimeoutMail(MonitorNodeVo node, HeartBeatVo hbConf, String flowLineCheckHtml) {
        IMail mail = DBusMailFactory.build();
        String subject = String.format("%s-%s.%s超时", timeout, dsNameWk, schemaNameWk);
        String contents = MsgUtil.format(Constants.MAIL_HEART_BEAT_NEW,
                "超时报警", dsNameWk, schemaNameWk,
                DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                IMail.ENV,
                flowLineCheckHtml,
                MsgUtil.format(AlarmResultContainer.getInstance().html(), html.toString()));
        Message msg = new Message();
        String projectRelatedEmail = getProjectRelatedTopologyDelayEmail(node.getDsName(), node.getSchema(), node.getTableName());
        if (StringUtils.isNotBlank(projectRelatedEmail)) {
            emailAddress += "," + projectRelatedEmail;
        }
        msg.setAddress(emailAddress);
        msg.setContents(contents);
        msg.setSubject(subject);
        msg.setPriority(priority);

        msg.setHost(hbConf.getAlarmMailSMTPAddress());
        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
        msg.setUserName(hbConf.getAlarmMailUser());
        msg.setPassword(hbConf.getAlarmMailPass());
        msg.setFromAddress(hbConf.getAlarmSendEmail());

        mail.send(msg);
        html.delete(0, html.length());
        emailAddress = "";
    }

    private String getProjectRelatedSlaveDelayEmail(String datasource, String schema, String table) {
        ILoadDbusConfigDao dao = new LoadDbusConfigDaoImpl();
        List<ProjectNotifyEmailsVO> emailsVOs = dao.queryRelatedNotifyEmails(Constants.CONFIG_DB_KEY, datasource, schema, table);
        Set<String> emails = new HashSet<>();
        for (ProjectNotifyEmailsVO emailsVO : emailsVOs) {
            if (emailsVO.getMasterSlaveDelayEmails() != null) {
                Collections.addAll(emails, emailsVO.getMasterSlaveDelayEmails());
            }
        }
        return StringUtils.join(emails, ",");
    }

    private String getProjectRelatedTopologyDelayEmail(String datasource, String schema, String table) {
        ILoadDbusConfigDao dao = new LoadDbusConfigDaoImpl();
        List<ProjectNotifyEmailsVO> emailsVOs = dao.queryRelatedNotifyEmails(Constants.CONFIG_DB_KEY, datasource, schema, table);
        Set<String> emails = new HashSet<>();
        for (ProjectNotifyEmailsVO emailsVO : emailsVOs) {
            if (emailsVO.getTopologyDelayEmails() != null) {
                Collections.addAll(emails, emailsVO.getTopologyDelayEmails());
            }
        }
        return StringUtils.join(emails, ",");
    }

}
