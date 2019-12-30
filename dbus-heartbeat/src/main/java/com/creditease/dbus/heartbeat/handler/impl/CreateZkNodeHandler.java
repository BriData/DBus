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
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class CreateZkNodeHandler extends AbstractHandler {

    @Override
    public void process() {
        //遍历检查或创建 所有在monitor里面的ZK 节点
        Set<MonitorNodeVo> nodes = HeartBeatConfigContainer.getInstance().getMonitorNodes();
        for (MonitorNodeVo node : nodes) {
            String[] dsPartitions = StringUtils.splitByWholeSeparator(node.getDsPartition(), ",");
            for (String partition : dsPartitions) {
                String path = HeartBeatConfigContainer.getInstance().getHbConf().getMonitorPath();
                path = StringUtils.join(new String[]{path, node.getDsName(), node.getSchema(), node.getTableName(), partition}, "/");
                CuratorContainer.getInstance().createZkNode(path);
            }
        }
    }
}
