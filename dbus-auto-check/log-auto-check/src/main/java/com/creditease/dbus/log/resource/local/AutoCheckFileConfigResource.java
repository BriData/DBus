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


package com.creditease.dbus.log.resource.local;


import com.creditease.dbus.log.bean.LogCheckConfigBean;

public class AutoCheckFileConfigResource extends FileConfigResource<LogCheckConfigBean> {

    public AutoCheckFileConfigResource(String name) {
        super(name);
    }

    @Override
    public LogCheckConfigBean parse() {
        LogCheckConfigBean conf = new LogCheckConfigBean();
        try {
            conf.setFilebeatBasePath(prop.getProperty("filebeat.base.path"));
            conf.setFilebeatDstTopic(prop.getProperty("filebeat.dst.topic"));
            conf.setFilebeatHeartBeatFilePath(prop.getProperty("filebeat.heartbeat.file.path"));
            conf.setFilebeatExtractFilePath(prop.getProperty("filebeat.extract.file.path"));

            conf.setFlumeBasePath(prop.getProperty("flume.base.path"));
            conf.setFlumeHost(prop.getProperty("flume.host"));
            conf.setFlumeDataSincedb(prop.getProperty("flume.data.sincedb"));
            conf.setFlumeHeartbeatSincedb(prop.getProperty("flume.heartbeat.sincedb"));
            conf.setFlumeExtractFilePath(prop.getProperty("flume.extract.file.path"));
            conf.setFlumeHeartBeatFilePath(prop.getProperty("flume.heartbeat.file.path"));
            conf.setFlumeDstTopic(prop.getProperty("flume.dst.topic"));

            conf.setLogstashBasePath(prop.getProperty("logstash.base.path"));
            conf.setLogstashExtractFilePath(prop.getProperty("logstash.extract.file.path"));
            conf.setLogstashSincedbPath(prop.getProperty("logstash.sincedb.path"));
            conf.setLogstashType(prop.getProperty("logstash.type"));
            conf.setLogstashFileStartPosition(prop.getProperty("logstash.file.start.position"));
            conf.setLogstashFilterType(prop.getProperty("logstash.filter.type"));
            conf.setLogstashDstTopic(prop.getProperty("logstash.dst.topic"));

            conf.setKafkaBootstrapServers(prop.getProperty("kafka.bootstrap.servers"));
            conf.setLogType(prop.getProperty("log.type"));

        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + name + " error!");
        }
        return conf;
    }

}
