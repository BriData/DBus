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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.MongoClientContainer;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.resource.IResource;
import com.creditease.dbus.heartbeat.resource.remote.DsConfigResource;
import com.creditease.dbus.heartbeat.resource.remote.MonitorNodeConfigResource;
import com.creditease.dbus.heartbeat.resource.remote.TargetTopicConfigResource;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.JdbcVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;

import org.apache.commons.collections.CollectionUtils;

/**
 * 加载DataSource和Schema的信息
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class LoadDbConfigHandler extends AbstractHandler {

    @Override
    public void process() {
        loadSid();
        loadMonitorNode();
        loadTargetTopic();
    }

    private void loadSid() {
        IResource<List<DsVo>> resource = new DsConfigResource(Constants.CONFIG_DB_KEY);
        List<DsVo> dsVos = resource.load();
        HeartBeatConfigContainer.getInstance().setDsVos(dsVos);
        if (CollectionUtils.isNotEmpty(dsVos)) {
            ConcurrentHashMap<String, DsVo> cmap = new ConcurrentHashMap<String, DsVo>();
            for (JdbcVo conf : dsVos) {
                cmap.put(conf.getKey(), (DsVo) conf);
                if (DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.LOG_LOGSTASH)
                        || DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.LOG_LOGSTASH_JSON)
                        || DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.LOG_UMS)
                        || DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.LOG_FLUME)
                        || DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.LOG_FILEBEAT)) {
                    continue;
                }
                if (DbusDatasourceType.stringEqual(conf.getType(), DbusDatasourceType.MONGO)) {
                    MongoClientContainer.getInstance().register(conf);
                    continue;
                }
                DataSourceContainer.getInstance().register(conf);
            }
            HeartBeatConfigContainer.getInstance().setCmap(cmap);
        }
    }

    private void loadMonitorNode() {
        IResource<Set<MonitorNodeVo>> resource = new MonitorNodeConfigResource(Constants.CONFIG_DB_KEY);
        Set<MonitorNodeVo> monitorNodes = resource.load();
        HeartBeatConfigContainer.getInstance().setMonitorNodes(monitorNodes);
    }

    private void loadTargetTopic() {
        IResource<Set<TargetTopicVo>> resource = new TargetTopicConfigResource(Constants.CONFIG_DB_KEY);
        Set<TargetTopicVo> topics = resource.load();
        /*for(TargetTopicVo topic : topics){
        	LoggerFactory.getLogger().info("[db-LoadDbusConfigDao] topic:{} ",topic.toString());
        }*/
        HeartBeatConfigContainer.getInstance().setTargetTopic(topics);
    }

}
