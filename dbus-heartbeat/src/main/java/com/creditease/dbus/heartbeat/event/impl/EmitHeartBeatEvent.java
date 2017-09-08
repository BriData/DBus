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

import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;

import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.dao.impl.HeartBeatDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.PacketVo;

/**
 *
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class EmitHeartBeatEvent extends AbstractEvent {

    private IHeartBeatDao dao;

    private int checkPointPerHeartBeatCnt = 5;

    public EmitHeartBeatEvent(long interval, CountDownLatch cdl, int checkPointPerHeartBeatCnt) {
        super(interval, cdl);
        dao = new HeartBeatDaoImpl();
        this.checkPointPerHeartBeatCnt = checkPointPerHeartBeatCnt;
    }

    @Override
    public void fire(DsVo ds, MonitorNodeVo node, String path, long txTime) {
        try {
            PacketVo packet = new PacketVo();
            packet.setNode(path);
            packet.setTime(System.currentTimeMillis());
            if (heartBeatCnt % checkPointPerHeartBeatCnt == 0) {
                packet.setType("checkpoint");
            } else {
                packet.setType("heartbeat");
            }
            packet.setTxTime(txTime);
            String strPacket = JsonUtil.toJson(packet);

            boolean isMysql = StringUtils.contains(ds.getDriverClass(), "mysql");
            int cnt = dao.sendPacket(ds.getKey(), node.getDsName(), node.getSchema(), node.getTableName(), strPacket, isMysql);
            if (cnt ==1 && isFirst) {
                saveZk(path, strPacket);
            }

            //emitCount++
            long emitCount = ds.getEmitCount();
            ds.setEmitCount(emitCount + 1);

            //删除不需要的心跳数据, 第一次emit一定会试图删除旧的
            if (emitCount % 1000 == 0) {
                dao.deleteOldHeartBeat(ds.getKey(), isMysql);
            }

            //LoggerFactory.getLogger().info("心跳数据发送{},数据包[{}].", (cnt == 1) ? "成功" : "失败", strPacket);
        } catch (Exception e) {
            LOG.error("[emit-heartbeat-event]", e);
        }
    }

}
