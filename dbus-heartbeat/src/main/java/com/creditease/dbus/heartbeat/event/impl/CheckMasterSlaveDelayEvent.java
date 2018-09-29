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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.dao.impl.HeartBeatDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatMonitorVo;
import com.creditease.dbus.heartbeat.vo.MasterSlaveDelayVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;

import org.apache.commons.lang.StringUtils;

/**
 * Created by mal on 2018/3/7.
 */
public class CheckMasterSlaveDelayEvent extends AbstractEvent {

    private IHeartBeatDao dao;

    public CheckMasterSlaveDelayEvent(long interval) {
        super(interval);
        dao = new HeartBeatDaoImpl();
    }

    @Override
    public void run() {
        List<DsVo> dsVos = HeartBeatConfigContainer.getInstance().getDsVos();
        Set<MonitorNodeVo> nodes = HeartBeatConfigContainer.getInstance().getMonitorNodes();
        while (isRun.get()) {
            try {
                if (isRun.get()) {
                    for (DsVo ds : dsVos) {
                        if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.LOG_LOGSTASH)
                                || DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.LOG_LOGSTASH_JSON)
                                || DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.LOG_UMS)
                                || DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.MONGO)
                                || DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.LOG_FILEBEAT)
                                || DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.LOG_FLUME)) {
                            LOG.info(ds.getType() + ", Ignored!");
                            continue;
                        }
                        String dsNameWk = StringUtils.EMPTY;
                        String schemaNameWk = StringUtils.EMPTY;
                        for (MonitorNodeVo node : nodes) {
                            //快速退出
                            if (!isRun.get())
                                break;

                            if (!StringUtils.equals(ds.getKey(), node.getDsName()))
                                continue;

                            if (!StringUtils.equals(dsNameWk, node.getDsName()) ||
                                !StringUtils.equals(schemaNameWk, node.getSchema())) {
                                // boolean isMysql = DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.MYSQL);
                                MasterSlaveDelayVo msdVo = new MasterSlaveDelayVo();
                                HeartBeatMonitorVo masterHbmVo = dao.queryLatestHeartbeat(ds.getKey(), node.getDsName(), node.getSchema(), ds.getType());
                                LOG.info("[check-master-slave-delay-event] key: {}, ds:{}, schema:{}, query result:{}",
                                        ds.getKey(), node.getDsName(), node.getSchema(), JsonUtil.toJson(masterHbmVo));
                                long masterLatestTime = -1l;
                                if (masterHbmVo != null) {
                                    msdVo.setMasterLatestTime(masterHbmVo.getCreateTime());
                                    if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.MYSQL)) {
                                        masterLatestTime = DateUtil.convertStrToLong4Date(masterHbmVo.getCreateTime());
                                    } else if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.ORACLE)) {
                                        masterLatestTime = DateUtil.convertStrToLong4Date(masterHbmVo.getCreateTime(), "yyyyMMdd HH:mm:ss.SSS");
                                    }
                                } else {
                                    msdVo.setMasterLatestTime(StringUtils.EMPTY);
                                }

                                if (StringUtils.isNotBlank(ds.getSlvaeUrl())) {
                                    String key = StringUtils.join(new String[] {ds.getKey(), "slave"}, "_");
                                    HeartBeatMonitorVo slaveHbmVo = dao.queryLatestHeartbeat(key, node.getDsName(), node.getSchema(), ds.getType());
                                    LOG.info("[check-master-slave-delay-event] key: {}, ds:{}, schema:{}, query result:{}",
                                            key, node.getDsName(), node.getSchema(), JsonUtil.toJson(slaveHbmVo));
                                    long slaveLatestTime = -1l;
                                    if (slaveHbmVo != null) {
                                        msdVo.setSlaveLatestTime(slaveHbmVo.getCreateTime());
                                        if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.MYSQL)) {
                                            slaveLatestTime = DateUtil.convertStrToLong4Date(slaveHbmVo.getCreateTime());
                                        } else if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.ORACLE)) {
                                            slaveLatestTime = DateUtil.convertStrToLong4Date(slaveHbmVo.getCreateTime(), "yyyyMMdd HH:mm:ss.SSS");
                                        }
                                        msdVo.setDiff(masterLatestTime - slaveLatestTime);
                                        msdVo.setStrDiff(DateUtil.diffDate(masterLatestTime, slaveLatestTime));
                                    } else {
                                        msdVo.setSlaveLatestTime(StringUtils.EMPTY);
                                    }
                                } else {
                                    LOG.warn("[check-master-slave-delay-event] key: {} of slave url empty.", ds.getKey());
                                }

                                String path = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                                path = StringUtils.join(new String[] {path, node.getDsName(), node.getSchema()}, "/");
                                MasterSlaveDelayVo preMsd = deserialize(path, MasterSlaveDelayVo.class);

                                if (preMsd != null) {
                                    // 说明主备延时已经刚刚追上,在zk上记录追上的时间
                                    if (preMsd.getDiff() != 0 && msdVo.getDiff() == 0) {
                                        msdVo.setSynTime(System.currentTimeMillis());
                                    }
                                }

                                if (msdVo.getDiff() != 0)
                                    msdVo.setSynTime(0l);
                                String msdJson = JsonUtil.toJson(msdVo);
                                LOG.info("[check-master-slave-delay-event] save zk data:{}", msdJson);
                                saveZk(path, msdJson);
                            }
                        }
                    }
                }
                sleep(interval, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error("[check-master-slave-delay-event]", e);
            }
        }
    }

}
