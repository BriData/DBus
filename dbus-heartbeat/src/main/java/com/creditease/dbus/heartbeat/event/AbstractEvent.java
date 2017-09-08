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

package com.creditease.dbus.heartbeat.event;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;

import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.impl.CheckHeartBeatEvent;
import com.creditease.dbus.heartbeat.event.impl.EmitHeartBeatEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.DsVo;

public abstract class AbstractEvent implements IEvent {

    protected Logger LOG = LoggerFactory.getLogger();

    protected long interval;

    protected boolean isFirst = true;

    protected CountDownLatch cdl;

    protected long heartBeatCnt = 0;

    protected volatile AtomicBoolean isRun = new AtomicBoolean(true);

    protected AbstractEvent(long interval) {
        this.interval = interval;
    }

    protected AbstractEvent(long interval, CountDownLatch cdl) {
        this.interval = interval;
        this.cdl = cdl;
    }

    @Override
    public void fire(DsVo ds, MonitorNodeVo node, String path, long txTime) {
    }

    @Override
    public void run() {
        List<DsVo> dsVos = HeartBeatConfigContainer.getInstance().getDsVos();
        Set<MonitorNodeVo> nodes = HeartBeatConfigContainer.getInstance().getMonitorNodes();
        long txTime = 0l;
        while (isRun.get()) {
            try {
                if (isRun.get()) {
                    if (this instanceof EmitHeartBeatEvent) {
                        heartBeatCnt++;
                        txTime = System.currentTimeMillis();
                        LOG.info("[control-event] 心跳次数:{}.", heartBeatCnt);
                    }
                    for (DsVo ds : dsVos) {
                        for (MonitorNodeVo node : nodes) {
                            //快速退出
                            if (!isRun.get())
                                break;

                            if (!StringUtils.equals(ds.getKey(), node.getDsName()))
                                continue;


                            String path = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                            path = StringUtils.join(new String[] {path, node.getDsName(), node.getSchema(), node.getTableName()}, "/");
                            if (this instanceof EmitHeartBeatEvent) {
                                fire(ds, node, path, txTime);
                                if (isFirst)
                                    cdl.countDown();
                            } else if (this instanceof CheckHeartBeatEvent) {
                                cdl.await();

                                String key = StringUtils.join(new String[] {node.getDsName(), node.getSchema()}, "/");
                                if (StringUtils.isBlank(EventContainer.getInstances().getSkipSchema(key))) {
                                    fire(ds, node, path, txTime);
                                } else {
                                    LOG.warn("[control-event] schema:{},正在拉取全量,{}不进行监控.", key, node.getTableName());
                                }
                            }
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

    @Override
    public void stop() {
        isRun.compareAndSet(true, false);
    }

    protected void sleep(long t, TimeUnit tu) {
        try {
            tu.sleep(t);
        } catch (InterruptedException e) {
            LOG.info("[control-event] 线程sleep:" + t + " " + tu.name() + "中被中断!");
        }
    }

    protected <T> T deserialize(String path, Class<T> clazz) throws Exception {
        T packet = null;
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        if (curator.getState() == CuratorFrameworkState.STOPPED) {
            LOG.info("[control-event] CuratorFrameworkState:{}", CuratorFrameworkState.STOPPED.name());
        } else {
            byte[] bytes = curator.getData().forPath(path);
            if (bytes != null && bytes.length != 0) {
                packet = JsonUtil.fromJson(new String(bytes, Charset.forName("UTF-8")),  clazz);
            }
        }
        return packet;
    }

    protected void saveZk(String node, String packet) {
        try {
            CuratorFramework curator = CuratorContainer.getInstance().getCurator();
            if (curator.getState() == CuratorFrameworkState.STOPPED) {
                LOG.info("[control-event] CuratorFrameworkState:{}", CuratorFrameworkState.STOPPED.name());
            } else {
                curator.setData().forPath(node, packet.getBytes());
            }
        } catch (Exception e) {
            LOG.error("[control-event] 报错znode: " + node + ",数据包:" + packet + "失败!", e);
        }
    }

}
 
