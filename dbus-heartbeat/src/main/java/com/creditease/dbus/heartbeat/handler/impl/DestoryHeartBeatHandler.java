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

import com.creditease.dbus.commons.ControlVo;
import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.type.CommandType;
import com.creditease.dbus.heartbeat.util.JsonUtil;

public class DestoryHeartBeatHandler extends AbstractHandler {

    @Override
    public void process() {
        try {
            String path = HeartBeatConfigContainer.getInstance().getHbConf().getControlPath();
            ControlVo ctrl = new ControlVo();
            ctrl.setCmdType(CommandType.DESTORY.getCommand());
            byte[] data = JsonUtil.toJson(ctrl).getBytes();
            CuratorContainer.getInstance().getCurator().setData().forPath(path, data);
        } catch (Exception e) {
            throw new RuntimeException("关闭heartbeat出错.", e);
        } finally {
            CuratorContainer.getInstance().close();
        }
    }

}
