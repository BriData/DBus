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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.creditease.dbus.commons.FullPullNodeDetailVo;
import com.creditease.dbus.components.sms.DBusSmsFactory;
import com.creditease.dbus.components.sms.ISms;
import com.creditease.dbus.components.sms.SmsMessage;
import com.creditease.dbus.components.sms.SmsType;
import com.creditease.dbus.heartbeat.container.AlarmResultContainer;
import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.dao.impl.LoadDbusConfigDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.event.AlarmType;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.CheckVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.ProjectNotifyEmailsVO;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;

/**
 * 用于检测全量拉取是否有超时。
 *
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class CheckFullPullEvent extends AbstractEvent {

    private Lock lock;

    public CheckFullPullEvent(long interval, Lock lock) {
        super(interval);
        this.lock = lock;
    }

    //获得zk上/DBUS/FullPuller所有叶子节点
    private void fetchZkNodeRecursively(String parentNode, List<String> resultNodesInfo, CuratorFramework curator) throws Exception {
        List<String> children = curator.getChildren().forPath(parentNode);
        if (!children.isEmpty()) {
            for (String curNode : children) {
                fetchZkNodeRecursively(parentNode + "/" + curNode, resultNodesInfo, curator);
            }
        } else {
            String ver = StringUtils.substringAfterLast(parentNode, "/");
            if (!ver.equalsIgnoreCase("null")) {
                resultNodesInfo.add(parentNode);
            }
        }
    }

    /**
     * 获取全量拉取的最新节点
     * @param fullPullerRootNode
     * @param curator
     * @return 子节点的全路径，之前非全路径，后面的path部分进行了拼接
     * @throws Exception
     */
    private List<String> filter(String fullPullerRootNode, CuratorFramework curator) throws Exception {
        List<String> flattedFullpullerNodeName = new ArrayList<>();

        //get all node list
        fetchZkNodeRecursively(fullPullerRootNode,flattedFullpullerNodeName, curator);

        HashMap<String, String> map = new HashMap<>();
        for (String znode : flattedFullpullerNodeName) {
            String key = StringUtils.substringBeforeLast(znode, "/");
            String ver = StringUtils.substringAfterLast(znode, "/");
            if (map.containsKey(key)) {
                String tempVersion = map.get(key);
                if (ver.compareTo(tempVersion) > 0) {
                    map.put(key, ver);
                }
            } else {
                map.put(key, ver);
            }
        }

        List<String> wkList = new ArrayList<String>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            wkList.add(StringUtils.join(new String[] {entry.getKey(), String.valueOf(entry.getValue())}, "/"));
        }
        return wkList;
    }

    @Override
    public void run() {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
        // 切片在kafka中的最大堆积数
        // long fullPullSliceMaxPending = hbConf.getFullPullSliceMaxPending();
        //监控全量拉取zk路径
        String monitorFullPullPath = hbConf.getMonitorFullPullPath();
        while (isRun.get()) {
            lock.lock();
            try {
                List<String> latestNodes = filter(monitorFullPullPath, curator);
                for (String znode :  latestNodes) {

                    /*if (!isRun.get())
                        break;
                    // 发序列化fullpull节点数据
                    FullPullNodeVo fpNode = deserialize(monitorFullPullPath, FullPullNodeVo.class);
                    if (fpNode == null)
                        continue;
                    if(fpNode.getConsumerOffset() == null){
                        LOG.warn("ConsumerOffset: " + fpNode.getConsumerOffset());
                        continue;
                    }
                    long diffVal = fpNode.getProducerOffset() - fpNode.getConsumerOffset();
                    //    LOG.info("全量拉取监控中,producer - consumer: " + diffVal);
                    if (diffVal > fullPullSliceMaxPending) {
                        LOG.warn("全量拉取过程中切片在kafka中的最大堆积数{},超过预警阀值{},生产者OffSet:{},消费者OffSet:{}.",
                                new Object[] {String.valueOf(diffVal), String.valueOf(fullPullSliceMaxPending),
                                        fpNode.getProducerOffset(), fpNode.getConsumerOffset()});
                    }*/

                    if (!isRun.get())
                        break;
                    //叶子节点路径（经过filter筛选后，已保持最新）
                    String path = znode;

                    //拆分叶子节点路径，提取datasource schema
                    String[] db_schema = StringUtils.split(znode, "/");

                    FullPullNodeDetailVo fpNodeDetail = deserialize(path, FullPullNodeDetailVo.class);
                    if (fpNodeDetail == null || StringUtils.isNotBlank(fpNodeDetail.getEndTime()))
                        continue;

                    /*if (StringUtils.isNotBlank(fpNodeDetail.getErrorMsg())) {
                        LOG.error("全量拉取执行过程中key:{},发生错误:{}.", path, fpNodeDetail.getErrorMsg());
                        // 准备通知storm
                        String type = ControlType.MONITOR_ALARM.name();
                        ControlMessage cm = new ControlMessage(System.currentTimeMillis(),
                                type, "heartbeat");
                        cm.addPayload(Constants.CONFIG_KAFKA_CONTROL_PAYLOAD_DB_KEY, db_schema[2]);
                        cm.addPayload(Constants.CONFIG_KAFKA_CONTROL_PAYLOAD_SCHEMA_KEY, db_schema[3]);
                        DsVo ds = HeartBeatConfigContainer.getInstance().getCmap().get(db_schema[2]);
                        if (ds != null) {
                            //KafkaUtil.send(cm.toJSONString(), ds.getCtrlTopic(), type);
                            //通知 增量继续这个功能有瑕疵，因为如果脏数据存在就会不停的通知。
                            //还是以 报警位置，此处 改为 输出log
                            LOG.error("发现全量拉取失败：{}", cm.toJSONString());
                        } else {
                            LOG.error("key:{},对应的数据源为null.", db_schema[2]);
                        }
                    }*/

                    if (!isRun.get())
                        break;

                    CheckVo check = AlarmResultContainer.getInstance().get(path);
                    if (check == null) {
                        check = new CheckVo();
                        check.setType(AlarmType.FULL_PULL);
                        check.setPath(path);
                        AlarmResultContainer.getInstance().put(path, check);
                    }

                    // 修正值(不同服务器时间不同步时使用)
                    long correcteValue = hbConf.getFullPullCorrecteValue();
                    // 比较时用当前时间
                    long currentTime = System.currentTimeMillis();
                    // 全量拉取超时时间
                    long fullPullTimeout = hbConf.getFullPullTimeout();
                    // 报警超时时间
                    long alarmTtl = hbConf.getAlarmTtl();
                    // 最大报警次数
                    int maxAlarmCnt = hbConf.getMaxAlarmCnt();
                    // 比较更新时间
                    long updTime = DateUtil.convertStrToLong4Date(fpNodeDetail.getUpdateTime());
                    // 差值
                    long diffVal = currentTime - updTime + correcteValue;

                    if (diffVal > fullPullTimeout) {

                     LOG.info("znode:{},修正值:{},当前时间:{},接收到时间{},差值:{} 秒",
                            new Object[] {path, String.valueOf(correcteValue), String.valueOf(currentTime),
                                    String.valueOf(updTime), String.valueOf((double)diffVal/1000)});

                        check.setNormal(false);

                        //超过alarm Ttl 可以再报
                        if( currentTime - check.getLastAlarmTime() > alarmTtl) {
                            check.setAlarmCnt(0);
                            check.setTimeoutCnt(check.getTimeoutCnt() + 1);
                        }

                        int alarmCnt = check.getAlarmCnt();
                        if (alarmCnt < maxAlarmCnt) {
                            check.setAlarmCnt(alarmCnt + 1);
                            check.setLastAlarmTime(currentTime); // TODO: 2016/11/30  modify to long

                            // 邮件和短信报警
                            String key = StringUtils.substringBeforeLast(path, "/");
                            key = StringUtils.substringBeforeLast(key, "/");
                            key = StringUtils.replace(key, "/DBus/FullPuller/", StringUtils.EMPTY);
                            String adminSMSNo = hbConf.getAdminSMSNo();
                            String adminUseSMS = hbConf.getAdminUseSMS();
                            String adminEmail = hbConf.getAdminEmail();
                            String adminUseEmail = hbConf.getAdminUseEmail();
                            Map<String, Map<String, String>> additionalNotify = hbConf.getAdditionalNotify();
                            Map<String, String> map = additionalNotify.get(key);

                            if (map == null) {
                                LOG.warn("[check-fullpull-event] 没有对key:{},短信和邮件报警进行配置.", key);
                            } else {
                                LOG.debug("[check-fullpull-event] key:{},的邮件和短信配置{}", key, JsonUtil.toJson(map));
                            }

                            String telNos = StringUtils.EMPTY;
                            if (StringUtils.equals(adminUseSMS.toUpperCase(), "Y")) {
                                telNos = adminSMSNo;
                            }
                            if (map != null && StringUtils.equals(map.get("UseSMS").toUpperCase(), "Y")) {
                                if (StringUtils.isNotBlank(telNos)) {
                                    telNos = StringUtils.join(new String[] {telNos, map.get("SMSNo")}, ",");
                                } else {
                                    telNos = map.get("SMSNo");
                                }
                            }
                            if (StringUtils.isNotBlank(telNos)) {
                                String contents = MessageFormat.format(Constants.MAIL_FULL_PULLER,
                                        path, check.getAlarmCnt(), check.getTimeoutCnt());
                                SmsMessage msg = new SmsMessage();
                                msg.setTelNo(telNos);
                                msg.setContents(contents);
                                msg.setSmsType(SmsType.FULL_PULLER.getName());
                                ISms sms = DBusSmsFactory.bulid();
                                sms.send(msg);
                            }

                            String email = StringUtils.EMPTY;
                            if (StringUtils.equals(adminUseEmail.toUpperCase(), "Y")) {
                                email = adminEmail;
                            }
                            if (map != null && StringUtils.equals(map.get("UseEmail").toUpperCase(), "Y")) {
                                if (StringUtils.isNotBlank(email)) {
                                    email = StringUtils.join(new String[] {email, map.get("Email")}, ",");
                                } else {
                                    email = map.get("Email");
                                }
                            }

                            if (ArrayUtils.getLength(db_schema) == 8) {
                                // eg: /DBus/FullPuller/Projects/P000123_12/db8_sh_s/NEWDX_SH/TEAM/2019-01-03 14.15.33.131 - 0
                                String projectRelatedEmail = getProjectRelatedFullpullEmail(db_schema[4], db_schema[5], db_schema[6]);
                                if (StringUtils.isNotBlank(projectRelatedEmail)) {
                                    email += "," + projectRelatedEmail;
                                }
                            }

                            if (StringUtils.isNotBlank(email)) {
                                LOG.info("[check-fullpull-event] 接收拉取全量邮件报警收件人EMail地址:{}.", email);
                                IMail mail = DBusMailFactory.build();
                                String subject = "DBus全量监控报警 ";
                                // String contents = MsgUtil.format(Constants.MAIL_FULL_PULLER, path, check.getAlarmCnt(), check.getTimeoutCnt());
                                String contents = StringUtils.EMPTY;
                                if (ArrayUtils.getLength(db_schema) == 6) {
                                    // eg: /DBus/FullPuller/db8_sh_s/NEWDX_SH/TEAM/2019-01-03 14.15.33.131 - 0
                                    contents = MsgUtil.format(Constants.MAIL_FULL_PULLER_NEW,
                                            "DBus全量监控报警", db_schema[2], db_schema[3], db_schema[4], db_schema[5],
                                            DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                                            IMail.ENV,
                                            fpNodeDetail.toHtml());
                                } else if (ArrayUtils.getLength(db_schema) == 8) {
                                    // eg: /DBus/FullPuller/Projects/P000123_12/db8_sh_s/NEWDX_SH/TEAM/2019-01-03 14.15.33.131 - 0
                                    contents = MsgUtil.format(Constants.MAIL_FULL_PULLER_NEW_PROJECT,
                                            "DBus全量监控报警", db_schema[3], db_schema[4], db_schema[5], db_schema[6], db_schema[7],
                                            DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                                            IMail.ENV,
                                            fpNodeDetail.toHtml());
                                }

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

                                mail.send(msg);
                            }

                            // 准备通知storm
                            /*String type = ControlType.MONITOR_ALARM.name();
                            ControlMessage cm = new ControlMessage(System.currentTimeMillis(),
                                    type, "heartbeat");
                            cm.addPayload(Constants.CONFIG_KAFKA_CONTROL_PAYLOAD_DB_KEY, db_schema[2]);
                            cm.addPayload(Constants.CONFIG_KAFKA_CONTROL_PAYLOAD_SCHEMA_KEY, db_schema[3]);
                            DsVo ds = HeartBeatConfigContainer.getInstance().getCmap().get(db_schema[2]);
                            if (ds != null) {
                                //KafkaUtil.send(cm.toJSONString(), ds.getCtrlTopic(), type);
                                //通知 增量继续这个功能有瑕疵，因为如果脏数据存在就会不停的通知。
                                //还是以 报警位置，此处 改为 输出log
                                LOG.error("发现全量拉取失败：{}", cm.toJSONString());

                            } else {
                                LOG.error("key:{}对应的数据源为null.", db_schema[2]);
                            }*/
                        }
                        LOG.info(check.toString());
                    } else {
                        check.setNormal(true);
                        check.setAlarmCnt(0);
                        check.setTimeoutCnt(0);
                        check.setLastAlarmTime(0);
                    }
                }
            } catch (Exception e) {
                LOG.error("[check-fullpull-event]", e);
            } finally {
                lock.unlock();
            }
            sleep(interval, TimeUnit.SECONDS);
        }
    }

    private String getProjectRelatedFullpullEmail(String datasource, String schema, String table) {
        ILoadDbusConfigDao dao = new LoadDbusConfigDaoImpl();
        List<ProjectNotifyEmailsVO> emailsVOs = dao.queryRelatedNotifyEmails(Constants.CONFIG_DB_KEY, datasource, schema, table);
        Set<String> emails = new HashSet<>();
        for (ProjectNotifyEmailsVO emailsVO : emailsVOs) {
            if (emailsVO.getFullPullerEmails() != null) {
                Collections.addAll(emails, emailsVO.getFullPullerEmails());
            }
        }
        return StringUtils.join(emails, ";");
    }

}
