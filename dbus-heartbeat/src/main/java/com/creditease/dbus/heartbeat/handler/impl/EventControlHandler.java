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

package com.creditease.dbus.heartbeat.handler.impl;

import com.creditease.dbus.heartbeat.event.impl.SendStatMessageEvent;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.creditease.dbus.heartbeat.container.EventContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.IEvent;
import com.creditease.dbus.heartbeat.event.impl.CheckFullPullEvent;
import com.creditease.dbus.heartbeat.event.impl.CheckHeartBeatEvent;
import com.creditease.dbus.heartbeat.event.impl.DeleteFullPullOldVersionEvent;
import com.creditease.dbus.heartbeat.event.impl.EmitHeartBeatEvent;
import com.creditease.dbus.heartbeat.event.impl.FullPullEndDelayEvent;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;

public class EventControlHandler extends AbstractHandler {

    @Override
    public void process() {

        //1 启动 发送心跳包 线程
        CountDownLatch cdl = new CountDownLatch(1);
        long heartbeatInterval = HeartBeatConfigContainer.getInstance().getHbConf().getHeartbeatInterval();
        int checkPointPerHeartBeatCnt = HeartBeatConfigContainer.getInstance().getHbConf().getCheckPointPerHeartBeatCnt();
        IEvent hbEvent = new EmitHeartBeatEvent(heartbeatInterval, cdl, checkPointPerHeartBeatCnt);
        Thread hbt = new Thread(hbEvent, "emit-heartbeat-event");
        hbt.start();
        EventContainer.getInstances().put(hbEvent, hbt);

        //2 启动 检查ZK心跳 线程
        long checkInterval = HeartBeatConfigContainer.getInstance().getHbConf().getCheckInterval();
        IEvent checkEvent = new CheckHeartBeatEvent(checkInterval, cdl);
        Thread chkt = new Thread(checkEvent, "check-heartbeat-event");
        chkt.start();
        EventContainer.getInstances().put(checkEvent, chkt);

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
    }

}
