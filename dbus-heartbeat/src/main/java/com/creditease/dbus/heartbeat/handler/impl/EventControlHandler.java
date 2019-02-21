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

package com.creditease.dbus.heartbeat.handler.impl;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.IEvent;
import com.creditease.dbus.heartbeat.event.impl.*;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.vo.DsVo;

import org.apache.commons.lang.StringUtils;

public class EventControlHandler extends AbstractHandler {

    @Override
    public void process() {

        //1 启动 发送心跳包 线程
        /*CountDownLatch cdl = new CountDownLatch(1);
        long heartbeatInterval = HeartBeatConfigContainer.getInstance().getHbConf().getHeartbeatInterval();
        int checkPointPerHeartBeatCnt = HeartBeatConfigContainer.getInstance().getHbConf().getCheckPointPerHeartBeatCnt();
        IEvent hbEvent = new EmitHeartBeatEvent(heartbeatInterval, cdl, checkPointPerHeartBeatCnt);
        Thread hbt = new Thread(hbEvent, "emit-heartbeat-event");
        hbt.start();
        EventContainer.getInstances().put(hbEvent, hbt);*/

        long heartbeatInterval = HeartBeatConfigContainer.getInstance().getHbConf().getHeartbeatInterval();
        int checkPointPerHeartBeatCnt = HeartBeatConfigContainer.getInstance().getHbConf().getCheckPointPerHeartBeatCnt();
        List<DsVo> dsVos = HeartBeatConfigContainer.getInstance().getDsVos();
        CountDownLatch cdl = new CountDownLatch(dsVos.size());
        for (DsVo ds : dsVos) {
            IEvent hbEvent = new EmitHeartBeatEvent(heartbeatInterval, cdl, checkPointPerHeartBeatCnt, ds.getKey());
            Thread hbt = new Thread(hbEvent, StringUtils.join(new String[] {"emit-heartbeat-event", ds.getKey()}, "-"));
            hbt.start();
            EventContainer.getInstances().put(hbEvent, hbt);
        }

        //2 启动 检查ZK心跳 线程
        long checkInterval = HeartBeatConfigContainer.getInstance().getHbConf().getCheckInterval();
        IEvent checkEvent = new CheckHeartBeatEvent(checkInterval, cdl);
        Thread chkt = new Thread(checkEvent, "check-heartbeat-event");
        chkt.start();
        EventContainer.getInstances().put(checkEvent, chkt);

        //2.1 启动 keeper的ZK心跳线程
        long keeperCheckInterval = HeartBeatConfigContainer.getInstance().getHbConf().getCheckInterval();
        IEvent checkProjectEvent = new ProjectCheckHeartBeatEvent(keeperCheckInterval,cdl);
        Thread chpkt = new Thread(checkProjectEvent,"check-project-heartbeat-event");
        chpkt.start();
        EventContainer.getInstances().put(checkProjectEvent, chpkt);

        //3 启动  检查拉全量 线程
        Lock lock = new ReentrantLock();
        long checkFullPullInterval = HeartBeatConfigContainer.getInstance().getHbConf().getCheckFullPullInterval();
        IEvent checkFullPullEvent = new CheckFullPullEvent(checkFullPullInterval, lock);
        Thread cfpEvent = new Thread(checkFullPullEvent, "check-fullpull-event");
        cfpEvent.start();
        EventContainer.getInstances().put(checkFullPullEvent, cfpEvent);

        //4 启动 删除 拉全量旧版本 线程
        long deleteFullPullOldVersionInterval = HeartBeatConfigContainer.getInstance().getHbConf().getDeleteFullPullOldVersionInterval();
        IEvent deleteFullPullOldVersionEvent = new DeleteFullPullOldVersionEvent(deleteFullPullOldVersionInterval, lock);
        Thread dfpovEvent = new Thread(deleteFullPullOldVersionEvent, "delete-fullpull-old-version-event");
        dfpovEvent.start();
        EventContainer.getInstances().put(deleteFullPullOldVersionEvent, dfpovEvent);

        //5 启动全量拉取结束延迟  线程
        IEvent fullPullEndDelayEvent = new FullPullEndDelayEvent(0l);
        Thread fpedEvent = new Thread(fullPullEndDelayEvent, "fullpull-end-delay-event");
        fpedEvent.start();
        EventContainer.getInstances().put(fullPullEndDelayEvent, fpedEvent);

        //6 启动Send Stat Message  线程
        IEvent sendStatMsgEvent = new SendStatMessageEvent(5l);
        Thread ssmEvent = new Thread(sendStatMsgEvent, "send-stat-msg-event");
        ssmEvent.start();
        EventContainer.getInstances().put(sendStatMsgEvent, ssmEvent);

        //7 启动 检测主备延时 线程
        long checkMasterSlaveDelayInterval = HeartBeatConfigContainer.getInstance().getHbConf().getCheckMasterSlaveDelayInterval();
        IEvent checkMasterSlaveDelayEvent = new CheckMasterSlaveDelayEvent(checkMasterSlaveDelayInterval);
        Thread cmsdEvent = new Thread(checkMasterSlaveDelayEvent, "check-master-slave-delay-event");
        cmsdEvent.start();
        EventContainer.getInstances().put(checkMasterSlaveDelayEvent, cmsdEvent);
    }

}
