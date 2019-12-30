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
import com.creditease.dbus.heartbeat.resource.IResource;
import com.creditease.dbus.heartbeat.resource.remote.HeartBeatConfigResource;
import com.creditease.dbus.heartbeat.type.WatcherType;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import org.apache.curator.framework.CuratorFramework;

/**
 * 加载zookeeper目录/dbus/heartbeat/config下配置信息
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class LoadZkConfigHandler extends AbstractHandler {

    @Override
    public void process() {
        try {
            IResource<HeartBeatVo> resource = new HeartBeatConfigResource();
            HeartBeatVo hbvo = resource.load();
            HeartBeatConfigContainer.getInstance().setHbConf(hbvo);

            /*IResource<MaasVo> resource_maas = new MaasConfigResource();
            MaasVo maasvo = resource_maas.load();
            HeartBeatConfigContainer.getInstance().setmaasConf(maasvo);*/

            // 注册控制事件
            CuratorContainer.getInstance().createZkNode(hbvo.getControlPath());
            CuratorFramework _curator = CuratorContainer.getInstance().getCurator();
            _curator.getData().usingWatcher(WatcherType.CONTROL).forPath(hbvo.getControlPath());
        } catch (Exception e) {
            throw new RuntimeException("加载zk配置时发生错误!", e);
        }
    }

}
