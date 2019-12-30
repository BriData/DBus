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
import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;
import com.creditease.dbus.heartbeat.resource.IResource;
import com.creditease.dbus.heartbeat.resource.local.CommonConfigResource;
import com.creditease.dbus.heartbeat.resource.local.JdbcFileConfigResource;
import com.creditease.dbus.heartbeat.resource.local.KafkaFileConfigResource;
import com.creditease.dbus.heartbeat.resource.local.ZkFileConfigResource;
import com.creditease.dbus.heartbeat.vo.CommonConfigVo;
import com.creditease.dbus.heartbeat.vo.JdbcVo;
import com.creditease.dbus.heartbeat.vo.ZkVo;

import java.util.List;
import java.util.Properties;

/**
 * 加载conf目录下的zk.properties,jdbc.properties,producer.properties配置文件
 *
 * @author Liang.Ma
 * @version 1.0
 */
public class LoadFileConfigHandler extends AbstractHandler {

    @Override
    public void process() {
        // 加载zk.properties
        loadZk();
        // 加载jdbc.properties
        loadJdbc();
        // 加载producer.properties
        loadKafkaProducer();
        //加载consumer.properties
        loadKafkaConsumer();
        // 加载config.properties
        loadConfig();
    }

    private void loadJdbc() {
        IResource<List<JdbcVo>> resource = new JdbcFileConfigResource("jdbc.properties");
        List<JdbcVo> jdbc = resource.load();
        HeartBeatConfigContainer.getInstance().setJdbc(jdbc);
        for (JdbcVo conf : jdbc)
            DataSourceContainer.getInstance().register(conf);
    }

    private void loadZk() {
        IResource<ZkVo> resource = new ZkFileConfigResource("zk.properties");
        ZkVo zk = resource.load();
        HeartBeatConfigContainer.getInstance().setZkConf(zk);
        CuratorContainer.getInstance().register(zk);
    }

    private void loadKafkaProducer() {
        IResource<Properties> resource = new KafkaFileConfigResource("producer.properties");
        Properties producer = resource.load();
        HeartBeatConfigContainer.getInstance().setKafkaProducerConfig(producer);
    }

    private void loadKafkaConsumer() {
        IResource<Properties> resource = new KafkaFileConfigResource("consumer.properties");
        Properties consumer = resource.load();
        HeartBeatConfigContainer.getInstance().setKafkaConsumerConfig(consumer);
    }

    private void loadConfig() {
        IResource<CommonConfigVo> resource = new CommonConfigResource("config.properties");
        CommonConfigVo configVo = resource.load();
        HeartBeatConfigContainer.getInstance().setConmmonConfig(configVo);
    }

}
