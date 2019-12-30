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


package com.creditease.dbus.heartbeat.handler.impl;

import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.BrokerInfoVo;
import com.creditease.dbus.heartbeat.vo.ZkVo;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

public class LeaderElectHandler extends AbstractHandler {

    @SuppressWarnings("resource")
    @Override
    public void process() {
        try {
            CuratorFramework _curator = CuratorContainer.getInstance().getCurator();
            ZkVo conf = HeartBeatConfigContainer.getInstance().getZkConf();
            CuratorContainer.getInstance().createZkNode(conf.getLeaderPath());

            // 获取进程ID和服务器hostName
            final BrokerInfoVo brokerInfo = new BrokerInfoVo();
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String[] pidAndHostName = StringUtils.split(name, "@");
            brokerInfo.setPid(pidAndHostName[0]);
            brokerInfo.setHostName(pidAndHostName[1]);
            brokerInfo.setIp(InetAddress.getLocalHost().getHostAddress());
            LeaderLatch ll = new LeaderLatch(_curator, conf.getLeaderPath(), JsonUtil.toJson(brokerInfo));

            ll.addListener(new LeaderLatchListener() {
                @Override
                public void notLeader() {
                    LoggerFactory.getLogger().error("本机现在切换到非leader状态,准备终止当前进程PID:{}", brokerInfo.getPid());
                    System.exit(-1);
                }

                @Override
                public void isLeader() {
                    LoggerFactory.getLogger().info("本机现在切换到leader状态.");
                }
            });

            ll.start();
            ll.await();
        } catch (Exception e) {
            throw new RuntimeException("选举leader时发生错误!", e);
        }
    }

}
