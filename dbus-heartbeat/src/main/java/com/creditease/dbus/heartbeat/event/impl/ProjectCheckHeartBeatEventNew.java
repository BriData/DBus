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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.creditease.dbus.heartbeat.container.AlarmResultContainer;
import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.dao.impl.LoadDbusConfigDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.event.AlarmType;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.CheckVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.MasterSlaveDelayVo;
import com.creditease.dbus.heartbeat.vo.PacketVo;
import com.creditease.dbus.heartbeat.vo.ProjectMonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.ProjectNotifyEmailsVO;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;

/**
 * 用于对之前对router监控不完善的全新修改
 *
 */
public class ProjectCheckHeartBeatEventNew extends AbstractEvent {

    private StringBuilder html = null;

    /**
     * project项目名称，对应唯一的project_name,非project_display_name
     */
    private String projectNameWk = StringUtils.EMPTY;

    /**
     * project下一级，topology的名称
     */
    private String topoNameWk = StringUtils.EMPTY;

    private String dsNameWk = StringUtils.EMPTY;

    private String schemaNameWk = StringUtils.EMPTY;

    private String tableNameWk = StringUtils.EMPTY;

    public ProjectCheckHeartBeatEventNew(long interval, CountDownLatch cdl) {
        super(interval, cdl);
        html = new StringBuilder();
    }

    @Override
    public void run() {

        //用来发送告警邮件时，查询邮件地址
        ILoadDbusConfigDao dao = new LoadDbusConfigDaoImpl();

        while (isRun.get()) {
            try {
                if (isRun.get()) {
                    //获取基础的path：/DBus/HeartBeat/ProjectMonitor
                    String basePath = HeartBeatConfigContainer.getInstance().getHbConf().getProjectMonitorPath();

                    //获取所有监控的节点
                    Set<String> monitorPaths = new HashSet<>();
                    monitorPaths = getZKNodePathsFromPath(basePath, monitorPaths);

                    //node节点存储 projectName，topoName,dsName,schemaName,dataName信息
                    ProjectMonitorNodeVo node = new ProjectMonitorNodeVo();

                    //遍历监控node
                    for (String path : monitorPaths) {
                        //快速退出
                        if (!isRun.get())
                            break;

                        //根据path信息初始化node
                        node = initNodeAttribute(node, path, basePath);
                        if (!checkNodeInfo(node)) {
                            continue;
                        }
                        cdl.await();
                        //根据dsName/schemaName判断，如果拉全量先不处理
                        String key = StringUtils.join(new String[]{node.getDsName(), node.getSchema()}, "/");
                        if (StringUtils.isBlank(EventContainer.getInstances().getSkipSchema(key))) {
                            fire2(node, path, dao);
                        } else {
                            LOG.warn("[control-event] schema:{},正在拉取全量,{}不进行监控.", key, node.getTableName());
                        }

                    }
                }
                sleep(interval, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error("[control-event]", e);
            }
            isFirst = false;
        }
    }

    /**
     * 延时告警
     *
     * @param node ProjectMonitorNodeVo
     * @param path :/DBus/HeartBeat/ProjectMonitor/project1/topo1/ds1.schema1.table1
     * @param dao  dao对象
     */
    public void fire2(ProjectMonitorNodeVo node, String path, ILoadDbusConfigDao dao) {
        try {
            ProjectNotifyEmailsVO emailsVO = dao.queryNotifyEmails(Constants.CONFIG_DB_KEY, node.getProjectName());
            //查询延时告警通知email信息
            String[] topologyDelayEmails = emailsVO.getTopologyDelayEmails();

            //如果告警通知关闭，那么直接返回
            if (topologyDelayEmails == null) {
                return;
            }

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

            //第一次发送为heartbeat发送时间,之后为接收心跳数据的时间
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


            if (notBlankAndEqual(projectNameWk, node, AttributeType.PROJECT_NAME)
                    && notBlankAndEqual(dsNameWk, node, AttributeType.DS_NAME)
                    && notBlankAndEqual(schemaNameWk, node, AttributeType.SCHEMA_NAME)) {

                /* 判断是超时还是主备，主备的略过 */

                long masterSlaveDelayTimeout = HeartBeatConfigContainer.getInstance().getHbConf().getMasterSlaveDelayTimeout();
                String masterSlaveDealyPath = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                //node生产策略改变，该字段抛弃
                //String[] dsSchemaName = tableNameWk.split("\\.");

                masterSlaveDealyPath = StringUtils.join(new String[]{masterSlaveDealyPath, dsNameWk, schemaNameWk}, "/");
                MasterSlaveDelayVo msdVo = deserialize(masterSlaveDealyPath, MasterSlaveDelayVo.class);
                long delayTime = 0l;
                long synTime = 0l;
                if (msdVo != null) {
                    delayTime = msdVo.getDiff() == null ? 0l : msdVo.getDiff();
                    synTime = msdVo.getSynTime() == null ? 0l : msdVo.getSynTime();
                }


                if (delayTime > masterSlaveDelayTimeout) {
                    html.delete(0, html.length());
                    return;
                } else if (html.length() > 0 && ((currentTime - synTime) > 1000 * 60 * 10)) {
                    // 为了防止主备库刚刚追上产生的延时报警, 给程序10分钟的一个追数据的时间
                    // 如果10分钟还有追上数据就报警

                    IMail mail = DBusMailFactory.build();
                    String subject = "DBus超时报警 ";
                    String contents = MsgUtil.format(Constants.MAIL_HEART_BEAT_NEW_PROJECT,
                            "超时报警", projectNameWk, topoNameWk, dsNameWk, schemaNameWk,
                            DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                            IMail.ENV,
                            MsgUtil.format(AlarmResultContainer.getInstance().html(), html.toString()));
                    //根据邮件地址集合，发送邮件
                    String emails = StringUtils.join(topologyDelayEmails, ",");

                    Message msg = new Message();
                    msg.setAddress(emails);
                    msg.setContents(contents);
                    msg.setSubject(subject);

                    msg.setHost(hbConf.getAlarmMailSMTPAddress());
                    if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                        msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                    msg.setUserName(hbConf.getAlarmMailUser());
                    msg.setPassword(hbConf.getAlarmMailPass());
                    msg.setFromAddress(hbConf.getAlarmSendEmail());

                    boolean ret = mail.send(msg);
                    LOG.info("[check-event] 发送邮件", ret == true ? "成功" : "失败");
                    html.delete(0, html.length());
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

                    LOG.info("[check-event] 接收心跳邮件报警收件人EMail地址:{}.", topologyDelayEmails);
                    html.append(check.html(DateUtil.diffDate(diffVal)));
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
            projectNameWk = node.getProjectName();
            topoNameWk = node.getTopoName();
            tableNameWk = node.getTableName();

        } catch (Exception e) {
            LOG.error("[check-event]", e);
        }
    }

    private enum AttributeType {PROJECT_NAME, TOPO_NAME, DS_NAME, SCHEMA_NAME, TABLE_NAME}

    /**
     * 需要判断的属性值不是blank,并且和节点属性值相等
     */
    private boolean notBlankAndEqual(String attribute, ProjectMonitorNodeVo node, AttributeType type) {
        String nodeAttribute = StringUtils.EMPTY;
        switch (type) {
            case PROJECT_NAME:
                nodeAttribute = node.getProjectName();
                break;
            case TOPO_NAME:
                nodeAttribute = node.getTopoName();
                break;
            case DS_NAME:
                nodeAttribute = node.getDsName();
                break;
            case SCHEMA_NAME:
                nodeAttribute = node.getSchema();
                break;
            case TABLE_NAME:
                nodeAttribute = node.getTopoName();
        }

        return StringUtils.isNotBlank(attribute) && StringUtils.equals(attribute, nodeAttribute);
    }


    /**
     * 根据根路径，获取所有末端叶子ZK节点
     *
     * @param basePath 根路径
     * @return
     * @throws Exception
     */
    private Set<String> getZKNodePathsFromPath(String basePath, Set<String> result) {
        Set<String> defaultSet = new HashSet<>();
        try {
            CuratorFramework curator = CuratorContainer.getInstance().getCurator();
            if (curator.getState() == CuratorFrameworkState.STOPPED) {
                LOG.info("[traverse zk node paths] CuratorFrameworkState:{}", CuratorFrameworkState.STOPPED.name());
                return defaultSet;
            } else {
                //获取下一级path
                List<String> paths = curator.getChildren().forPath(basePath);
                if (paths.size() > 0) {
                    //遍历添加下一级节点
                    for (String path : paths) {
                        path = basePath + "/" + path;
                        getZKNodePathsFromPath(path, result);
                    }
                } else {
                    //如果是末端叶子节点，那么将完整的路径加入到结果集
                    if (!HeartBeatConfigContainer.getInstance().getHbConf().getProjectMonitorPath()
                            .equals(basePath)) {// 默认的base path不加入
                        LOG.info("add nodePath : " + basePath);
                        result.add(basePath);
                    }

                }
                return result;
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[ traverse zk node paths] path :" + basePath, e);
            return defaultSet;
        }
    }

    /**
     * 从path中提取信息，放入node中
     *
     * @param node
     * @param path     原始path: /DBus/HeartBeat/ProjectMonitor/project1/topo1/ds1.schema1.table1
     * @param basePath
     * @return
     */
    private ProjectMonitorNodeVo initNodeAttribute(ProjectMonitorNodeVo node, String path, String basePath) {
        //操作后path：project1/topo1/ds1.schema1.table1
        path = StringUtils.replace(path, basePath + "/", StringUtils.EMPTY);

        //projectTopoName:project1/topo1
        String projectTopoName = StringUtils.substringBeforeLast(path, "/");
        node.setProjectName(StringUtils.substringBeforeLast(projectTopoName, "/"));
        node.setTopoName(StringUtils.substringAfterLast(projectTopoName, "/"));

        //ds1.schema1.table1
        String leafNodeName = StringUtils.substringAfterLast(path, "/");
        node.setTableName(StringUtils.substringAfterLast(path, "."));

        //ds1.schema1
        String datasourceSchemaName = StringUtils.substringBeforeLast(leafNodeName, ".");
        node.setDsName(StringUtils.substringBefore(datasourceSchemaName, "."));
        node.setSchema(StringUtils.substringAfter(datasourceSchemaName, "."));

        LOG.info(MsgUtil.format("node info: project: {0}, topo: {1}, ds: {2}, schema: {3}, table: {4}",
                node.getProjectName(), node.getTopoName(), node.getDsName(), node.getSchema(), node.getTableName()));
        return node;
    }

    private boolean checkNodeInfo(ProjectMonitorNodeVo node) {
        if (StringUtils.isBlank(node.getProjectName()) || StringUtils.isBlank(node.getTopoName())
                || StringUtils.isBlank(node.getDsName()) || StringUtils.isBlank(node.getSchema())
                || StringUtils.isBlank(node.getTableName())) {
            LOG.info(MsgUtil.format("check node info error: project: {0}, topo: {1}, ds: {2}, schema: {3}, table: {4}",
                    node.getProjectName(), node.getTopoName(), node.getDsName(), node.getSchema(), node.getTableName()));
            return false;
        }
        return true;
    }

}
