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


package com.creditease.dbus.heartbeat.resource.local;

import com.creditease.dbus.heartbeat.vo.ZkVo;

public class ZkFileConfigResource extends FileConfigResource<ZkVo> {

    public ZkFileConfigResource(String name) {
        super(name);
    }

    @Override
    public ZkVo parse() {
        ZkVo conf = new ZkVo();
        try {
            conf.setZkStr(prop.getProperty("zk.str"));
            conf.setZkConnectionTimeout(Integer.parseInt(prop.getProperty("zk.session.timeout")));
            conf.setZkSessionTimeout(Integer.parseInt(prop.getProperty("zk.connection.timeout")));
            conf.setZkRetryInterval(Integer.parseInt(prop.getProperty("zk.retry.interval")));
            conf.setConfigPath(prop.getProperty("dbus.heartbeat.config.path"));
            conf.setLeaderPath(prop.getProperty("dbus.heartbeat.leader.path"));
            conf.setMaas_configPath(prop.getProperty("dbus.heartbeat.maas.config.path"));
            conf.setMaas_consumerPath(prop.getProperty("dbus.heartbeat.maas.consumer.path"));
            conf.setMaas_producerPath(prop.getProperty("dbus.heartbeat.maas.producer.path"));
        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + name + " error!");
        }
        return conf;
    }

}
