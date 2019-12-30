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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.vo.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class HeartBeatConfigContainer {

    private static HeartBeatConfigContainer container;

    private List<JdbcVo> jdbc;

    private List<DsVo> dsVos;

    private ZkVo zkConf;

    private HeartBeatVo hbConf;

    private MaasVo maasConf;

    private Set<MonitorNodeVo> monitorNodes;

    private Map<String, String> aliasMapping;

    private Properties kafkaProducerConfig;

    private Properties kafkaConsumerConfig;

    private CommonConfigVo conmmonConfig;

    private Set<TargetTopicVo> targetTopic;

    private ConcurrentHashMap<String, DsVo> cmap;

    private HeartBeatConfigContainer() {
    }

    public static HeartBeatConfigContainer getInstance() {
        if (container == null) {
            synchronized (HeartBeatConfigContainer.class) {
                if (container == null)
                    container = new HeartBeatConfigContainer();
            }
        }
        return container;
    }

    public List<JdbcVo> getJdbc() {
        return jdbc;
    }

    public void setJdbc(List<JdbcVo> jdbc) {
        this.jdbc = jdbc;
    }

    public List<DsVo> getDsVos() {
        return dsVos;
    }

    public void setDsVos(List<DsVo> dsVos) {
        this.dsVos = dsVos;
    }

    public ZkVo getZkConf() {
        return zkConf;
    }

    public void setZkConf(ZkVo zkConf) {
        this.zkConf = zkConf;
    }

    public Set<MonitorNodeVo> getMonitorNodes() {
        return monitorNodes;
    }

    public void setMonitorNodes(Set<MonitorNodeVo> monitorNodes) {
        this.monitorNodes = monitorNodes;
    }

    public HeartBeatVo getHbConf() {
        return hbConf;
    }

    public void setHbConf(HeartBeatVo hbConf) {
        this.hbConf = hbConf;
    }

    public MaasVo getmaasConf() {
        return maasConf;
    }

    public void setmaasConf(MaasVo maasConf) {
        this.maasConf = maasConf;
    }

    public Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    public void setKafkaProducerConfig(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public Properties getKafkaConsumerConfig() {
        return kafkaConsumerConfig;
    }

    public void setKafkaConsumerConfig(Properties kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    public CommonConfigVo getConmmonConfig() {
        return conmmonConfig;
    }

    public void setConmmonConfig(CommonConfigVo conmmonConfig) {
        this.conmmonConfig = conmmonConfig;
    }

    public Set<TargetTopicVo> getTargetTopic() {
        return targetTopic;
    }

    public void setTargetTopic(Set<TargetTopicVo> targetTopic) {
        this.targetTopic = targetTopic;
    }

    public ConcurrentHashMap<String, DsVo> getCmap() {
        return cmap;
    }

    public void setCmap(ConcurrentHashMap<String, DsVo> cmap) {
        this.cmap = cmap;
    }

    public Map<String, String> getAliasMapping() {
        return aliasMapping;
    }

    public void setAliasMapping(Map<String, String> aliasMapping) {
        this.aliasMapping = aliasMapping;
    }

    public void clear() {
        jdbc = null;
        dsVos = null;
        zkConf = null;
        hbConf = null;
        monitorNodes = null;
        kafkaProducerConfig = null;
        targetTopic = null;
        cmap = null;
        aliasMapping = null;
    }
}
