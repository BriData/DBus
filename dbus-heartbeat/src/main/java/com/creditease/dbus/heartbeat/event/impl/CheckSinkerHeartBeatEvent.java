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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.heartbeat.container.AlarmResultContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.sinker.SinkerKafkaSource;
import com.creditease.dbus.heartbeat.sinker.SinkerMonitorNode;
import com.creditease.dbus.heartbeat.sinker.SinkerMonitorNodeManager;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.CommonConfigVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class CheckSinkerHeartBeatEvent extends AbstractEvent {

    private SinkerKafkaSource source = null;
    private SinkerMonitorNodeManager monitorNodeManager = null;
    private int checkAlarmInterval;
    private long lastCheckAlarmTime;
    private HashMap<String, SinkerMonitorNode> alarmNodes;
    private HeartBeatVo hbConf = null;
    private CommonConfigVo commonConfig = null;
    private Map<String, Map<String, String>> heartBeatTimeoutConf = null;
    // 最大报警次数
    private int maxAlarmCnt = 0;
    // 报警间隔, 超过alarm Ttl 可以再报
    private long alarmTtl = 0;
    private Map<String, Map<String, String>> additionalConf = null;

    public CheckSinkerHeartBeatEvent(long interval) {
        super(interval);
    }

    @Override
    public void run() {
        try {
            init();
            source = new SinkerKafkaSource();
            List<String> list;
            while (isRun.get()) {
                try {
                    checkAlarmTime();
                    list = source.poll();
                    if (list == null) {
                        continue;
                    }
                    //yxorcl.YX_USER.T_CONNECTION_CHECK|1572508713387|1573456732417|948019030
                    list.forEach(s -> {
                        String key = s.substring(0, s.indexOf("|"));
                        int latencyMS = Integer.parseInt(s.substring(s.lastIndexOf("|") + 1));
                        monitorNodeManager.update(key, latencyMS);
                    });
                } catch (Exception e) {
                    LOG.error("[sinker] {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            LOG.error("[sinker]", e);
        } finally {
            if (source != null) {
                source.cleanUp();
                source = null;
            }
            LOG.info("[sinker] exit.");
        }
    }

    private void init() {
        Set<MonitorNodeVo> nodes = HeartBeatConfigContainer.getInstance().getMonitorNodes();
        this.monitorNodeManager = new SinkerMonitorNodeManager();
        this.hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
        this.commonConfig = HeartBeatConfigContainer.getInstance().getConmmonConfig();
        this.checkAlarmInterval = Integer.parseInt(commonConfig.getSinkerCheckAlarmInterval());
        this.alarmNodes = new HashMap<>();
        nodes.forEach(monitorNodeVo -> monitorNodeManager.add(String.format("%s.%s.%s", monitorNodeVo.getDsName(), monitorNodeVo.getSchema(), monitorNodeVo.getTableName())));
        // 心跳超时补充配置
        this.heartBeatTimeoutConf = new HashMap<>();
        additionalConf = hbConf.getHeartBeatTimeoutAdditional();
        if (additionalConf != null) {
            for (Map.Entry<String, Map<String, String>> entry : additionalConf.entrySet()) {
                HashMap<String, String> map = new HashMap<>();
                map.put("startTime", entry.getValue().get("startTime"));
                map.put("endTime", entry.getValue().get("endTime"));
                map.put("heartBeatTimeout", entry.getValue().get("heartBeatTimeout"));
                heartBeatTimeoutConf.put(entry.getKey(), map);
            }
        } else {
            additionalConf = new HashMap<>();
        }
        // 最大报警次数
        maxAlarmCnt = hbConf.getMaxAlarmCnt();
        // 报警间隔, 超过alarm Ttl 可以再报
        alarmTtl = hbConf.getAlarmTtl();
    }

    private void checkAlarmTime() {
        if ((System.currentTimeMillis() - lastCheckAlarmTime) > checkAlarmInterval) {
            long heartBeatTimeout = hbConf.getHeartBeatTimeout();
            Map<String, SinkerMonitorNode> sinkerMonitorMap = monitorNodeManager.getSinkerMonitorMap();
            sinkerMonitorMap.forEach((key, sinkerMonitorNode) -> {
                if (sinkerMonitorNode.isRunning()) {
                    boolean canFire = false;
                    //延迟时间 = 上一次sinker心跳延时时间 + 上次更新距离现在的时间差
                    long latencyMS = sinkerMonitorNode.getLatencyMS() + System.currentTimeMillis() - sinkerMonitorNode.getUpdateTime();
                    sinkerMonitorNode.setRealLatencyMS(latencyMS);
                    //报警成立条件,1.超时了,报警次数少于最大允许次数;2.超时了,报警次数大于等于最大允许次数,上次报警时间距离现在超过了报警间隔
                    if (latencyMS > heartBeatTimeout && (maxAlarmCnt < sinkerMonitorNode.getAlarmCount()
                            || (maxAlarmCnt >= sinkerMonitorNode.getAlarmCount() && alarmTtl < System.currentTimeMillis() - sinkerMonitorNode.getLastAlarmTime()))) {
                        // 修正超时时间
                        Map<String, String> map = additionalConf.get(key.substring(0, key.lastIndexOf(".")));
                        if (map != null && DateUtil.isCurrentTimeInInterval(map.get("startTime"), map.get("endTime"))) {
                            if (latencyMS > Long.parseLong(map.get("heartBeatTimeout"))) {
                                alarmNodes.put(key, sinkerMonitorNode);
                                canFire = true;
                            }
                        } else {
                            alarmNodes.put(key, sinkerMonitorNode);
                            canFire = true;
                        }

                        //处理报警节点计数
                        if (canFire) {
                            LOG.info("[sinker] 表[{}]发生超时 ,超时时间:{},{}", key, DateUtil.diffDate(sinkerMonitorNode.getRealLatencyMS()), JSON.toJSONString(sinkerMonitorNode));
                            if (maxAlarmCnt < sinkerMonitorNode.getAlarmCount()) {
                                sinkerMonitorNode.setAlarmCount(sinkerMonitorNode.getTimeoutCnt() + 1);
                            } else {
                                sinkerMonitorNode.setAlarmCount(1);
                            }
                            sinkerMonitorNode.setTimeoutCnt(sinkerMonitorNode.getTimeoutCnt() + 1);
                            sinkerMonitorNode.setLastAlarmTime(System.currentTimeMillis());
                        }
                    } else {
                        sinkerMonitorNode.setTimeoutCnt(0);
                    }
                }
            });
            LOG.info("[sinker] checkAlarmTime complete.");
            this.lastCheckAlarmTime = System.currentTimeMillis();
        }
        if (!alarmNodes.isEmpty()) {
            // 根据schema进行分组发邮件,防止一封邮件行数太多
            ConcurrentMap<String, List<String>> schemas = alarmNodes.keySet().stream().collect(
                    Collectors.groupingByConcurrent(key -> key.substring(0, key.lastIndexOf("."))));
            schemas.values().forEach(value -> sendEmail(value));
            alarmNodes.clear();
        }
    }

    private void sendEmail(List<String> value) {
        String html = toHtml(value);
        String adminEmail = hbConf.getAdminEmail();

        String subject = "DBus Sinker超时报警 ";
        String contents = MsgUtil.format(Constants.MAIL_SINKER_HEART_BEAT_NEW,
                "超时报警",
                DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                IMail.ENV,
                MsgUtil.format(AlarmResultContainer.getInstance().html(), html));
        Message msg = new Message();
        msg.setAddress(adminEmail);
        msg.setContents(contents);
        msg.setSubject(subject);

        msg.setHost(hbConf.getAlarmMailSMTPAddress());
        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
        msg.setUserName(hbConf.getAlarmMailUser());
        msg.setPassword(hbConf.getAlarmMailPass());
        msg.setFromAddress(hbConf.getAlarmSendEmail());

        IMail mail = DBusMailFactory.build();
        mail.send(msg);
    }

    public String toHtml(List<String> tables) {
        Collections.sort(tables);
        StringBuilder html = new StringBuilder();
        tables.forEach(key -> {
            SinkerMonitorNode sinkerMonitorNode = alarmNodes.get(key);
            html.append("<tr bgcolor=\"#ffffff\">");
            html.append("    <th align=\"left\">" + key + "</th>");
            html.append("    <th align=\"right\">" + sinkerMonitorNode.getAlarmCount() + "</th>");
            html.append("    <th align=\"right\">" + DateUtil.diffDate(sinkerMonitorNode.getRealLatencyMS()) + "</th>");
            html.append("    <th align=\"right\">" + sinkerMonitorNode.getTimeoutCnt() + "</th>");
            html.append("</tr>");
        });
        return html.toString();
    }

    private void sendEmail() {

        String html = toHtml();
        String adminEmail = hbConf.getAdminEmail();

        String subject = "DBus Sinker超时报警 ";
        String contents = MsgUtil.format(Constants.MAIL_SINKER_HEART_BEAT_NEW,
                "超时报警",
                DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                IMail.ENV,
                MsgUtil.format(AlarmResultContainer.getInstance().html(), html));
        Message msg = new Message();
        msg.setAddress(adminEmail);
        msg.setContents(contents);
        msg.setSubject(subject);

        msg.setHost(hbConf.getAlarmMailSMTPAddress());
        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
        msg.setUserName(hbConf.getAlarmMailUser());
        msg.setPassword(hbConf.getAlarmMailPass());
        msg.setFromAddress(hbConf.getAlarmSendEmail());

        IMail mail = DBusMailFactory.build();
        mail.send(msg);
    }

    public String toHtml() {
        ArrayList<String> keys = new ArrayList<>(alarmNodes.keySet());
        Collections.sort(keys);
        StringBuilder html = new StringBuilder();
        keys.forEach(key -> {
            SinkerMonitorNode sinkerMonitorNode = alarmNodes.get(key);
            html.append("<tr bgcolor=\"#ffffff\">");
            html.append("    <th align=\"left\">" + key + "</th>");
            html.append("    <th align=\"right\">" + sinkerMonitorNode.getAlarmCount() + "</th>");
            html.append("    <th align=\"right\">" + DateUtil.diffDate(sinkerMonitorNode.getRealLatencyMS()) + "</th>");
            html.append("    <th align=\"right\">" + sinkerMonitorNode.getTimeoutCnt() + "</th>");
            html.append("</tr>");
        });
        return html.toString();
    }

}
