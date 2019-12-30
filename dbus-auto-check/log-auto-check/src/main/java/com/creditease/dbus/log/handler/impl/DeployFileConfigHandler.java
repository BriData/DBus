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


package com.creditease.dbus.log.handler.impl;


import com.creditease.dbus.log.bean.LogCheckConfigBean;
import com.creditease.dbus.log.container.AutoCheckConfigContainer;
import com.creditease.dbus.log.handler.AbstractHandler;
import com.creditease.dbus.log.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;


public class DeployFileConfigHandler extends AbstractHandler {

    @Override
    public void checkDeploy(BufferedWriter bw) throws Exception {
        LogCheckConfigBean lccb = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String logType = lccb.getLogType();
        if (StringUtils.equals(logType, "filebeat")) {
            deployFilebeat(lccb, bw);
        } else if (StringUtils.equals(logType, "flume")) {
            deployFlume(lccb, bw);
        } else if (StringUtils.equals(logType, "logstash")) {
            deployLogstash(lccb, bw);
        } else {
            bw.write("日志类型错误！请检查配置项!\n");
            System.out.println("ERROR: 日志类型错误！请检查log.type配置项！");
            updateConfigProcessExit(bw);
        }
    }

    private void deployFilebeat(LogCheckConfigBean lccb, BufferedWriter bw) throws IOException {
        String kafkaBootstrapServers = lccb.getKafkaBootstrapServers();
        String filebeatExtractFilePath = lccb.getFilebeatExtractFilePath();
        String filebeatBasePath = lccb.getFilebeatBasePath();
        String dstTopic = lccb.getFilebeatDstTopic();

        String filebeatConfigFilePath = StringUtils.joinWith("/", filebeatBasePath, "filebeat.yml");
        File filebeatConfigFile = new File(filebeatConfigFilePath);
        String filebeatTemplateFilePath = StringUtils.joinWith("/", filebeatBasePath, "filebeat.yml.template");
        File filebeatTemplateFile = new File(filebeatTemplateFilePath);

        //利用文件模板，仅对多个抽取路径有效
        //1. 删除所要修改的配置文件
        if (filebeatConfigFile.exists())
            FileUtils.deleteFile(filebeatConfigFilePath);

        //2. 复制要修改的配置文件
        if (!filebeatTemplateFile.exists()) {
            System.out.println("ERROR: [" + filebeatTemplateFile + "] is not exist!");
            bw.write(filebeatTemplateFile + " is not exist!");
            updateConfigProcessExit(bw);
        }

        FileUtils.copyFileUsingFileChannels(filebeatTemplateFile, filebeatConfigFile);

        //3. 对复制文件追加内容
        String[] extractFilePaths = StringUtils.split(filebeatExtractFilePath, ",");
        //构造多个抽取路径
        for (int i = 0; i < extractFilePaths.length; i++) {
            File file = new File(extractFilePaths[i]);
            if (FileUtils.checkFileAndFolderIsExist(extractFilePaths[i])) {
                String filebeatFileExtractSection = buildFilebeatFileExtractSection(extractFilePaths[i]);
                FileUtils.insert(filebeatConfigFilePath, 22, filebeatFileExtractSection);
            } else {
                updateConfigProcessExit(bw);
            }
        }

        //修改kafka地址
        String oldKafkaStr = "hosts: ";
        String newKafkaStr = buildFilebeatKafkaBrokerConfig(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(filebeatConfigFilePath + "/", oldKafkaStr, newKafkaStr, bw);
        } catch (IOException e) {
            updateConfigException(bw, filebeatConfigFilePath, oldKafkaStr, newKafkaStr, e);
        }

        //修改topic
        String oldTopicStr = "topic: ";
        String newTopicStr = buildFilebeatKafkaTopicConfig(dstTopic);
        try {
            FileUtils.modifyFileProperties(filebeatConfigFilePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            updateConfigException(bw, filebeatConfigFilePath, oldTopicStr, newTopicStr, e);
        }

        bw.write("更新filebeat配置成功！\n");
        System.out.println("更新filebeat配置成功！\n");
    }

    private void updateConfigException(BufferedWriter bw, String fileConfigFilePath, String oldTopicStr, String newTopicStr, IOException e) throws IOException {
        System.out.println("修改文件配置出错！fileBasePath： " + fileConfigFilePath + "oldTopicStr: " + oldTopicStr + "newTopicStr: " + newTopicStr);
        System.out.println("Error Message: " + e + "\n");
        updateConfigProcessExit(bw);
    }

    private void deployFlume(LogCheckConfigBean lccb, BufferedWriter bw) throws IOException {
        String kafkaBootstrapServers = lccb.getKafkaBootstrapServers();
        String flumeBasePath = lccb.getFlumeBasePath();
        String flumeHost = lccb.getFlumeHost();
        String flumeDataSincedb = lccb.getFlumeDataSincedb();
        String flumeHeartbeatSincedb = lccb.getFlumeHeartbeatSincedb();
        String flumeHeartbeatFilePath = lccb.getFlumeHeartBeatFilePath();
        String flumeExtractFilePath = lccb.getFlumeExtractFilePath();
        String flumeDstTopic = lccb.getFlumeDstTopic();


        String flumeConfigFilePath = StringUtils.joinWith("/", flumeBasePath, "conf/flume-conf.properties");
        File flumeConfigFile = new File(flumeConfigFilePath);
        String flumeTemplateFilePath = StringUtils.joinWith("/", flumeBasePath, "conf/flume-conf.properties.template");
        File flumeTemplateFile = new File(flumeTemplateFilePath);

        //利用文件模板
        //1. 删除所要修改的配置文件
        if (flumeConfigFile.exists())
            FileUtils.deleteFile(flumeConfigFilePath);

        //2. 复制要修改的配置文件
        if (!flumeTemplateFile.exists()) {
            System.out.println("ERROR: [" + flumeConfigFilePath + "] is not exist!");
            bw.write(flumeConfigFilePath + " is not exist!");
            updateConfigProcessExit(bw);
        }

        FileUtils.copyFileUsingFileChannels(flumeTemplateFile, flumeConfigFile);

        if (!flumeConfigFile.exists()) {
            System.out.println("ERROR: [" + flumeConfigFilePath + "] is not exist!");
            bw.write(flumeConfigFilePath + " is not exist!");
            updateConfigProcessExit(bw);
        }

        //替换心跳包
        String oldHeartbeatStr = "agent.sources.r_hb_0.interceptors.i_sr_2.replaceString";
        String newHeartbeatStr = buildFlumeHeartbeat(flumeHost);
        try {
            FileUtils.modifyFileProperties(flumeConfigFilePath, oldHeartbeatStr, newHeartbeatStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + "oldHeartbeatStr: " + oldHeartbeatStr + "newHeartbeatStr: " + newHeartbeatStr);
            bw.write("修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldHeartbeatStr: " + oldHeartbeatStr + " newHeartbeatStr: " + newHeartbeatStr);
            updateConfigProcessExit(bw);
        }


        File flumeExtractFile = new File(flumeExtractFilePath);
        if (FileUtils.checkFileAndFolderIsExist(flumeExtractFilePath)) {
            //替换抽取文件路径，flume比较特殊，目前只允许配置一个文件路径，可以是文件夹
            String oldExtractFileStr = "agent.sources.r_hb_0.filegroups.hblf";
            String newExtractFileStr = buildFlumeExtractFileStr(flumeExtractFilePath);
            try {
                FileUtils.modifyFileProperties(flumeConfigFilePath, oldExtractFileStr, newExtractFileStr, bw);
            } catch (IOException e) {
                System.out.println("ERROR: 修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + "oldExtractFileStr: " + oldExtractFileStr + "newExtractFileStr: " + newExtractFileStr);
                bw.write("修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + "oldExtractFileStr: " + oldExtractFileStr + "newExtractFileStr: " + newExtractFileStr);
                updateConfigProcessExit(bw);
            }
        } else {
            updateConfigProcessExit(bw);
        }


        if (FileUtils.checkFileAndFolderIsExist(flumeHeartbeatFilePath)) {
            //替换心跳日志文件路径
            String oldHBFilePathStr = "agent.sources.r_dahb.filegroups.dahblf";
            String newHBFilePathStr = buildFlumeHBExtractFilePathStr(flumeHeartbeatFilePath);
            try {
                FileUtils.modifyFileProperties(flumeConfigFilePath, oldHBFilePathStr, newHBFilePathStr, bw);
            } catch (IOException e) {
                System.out.println("ERROR: 修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldHBFilePathStr: " + oldHBFilePathStr + " newHBFilePathStr: " + newHBFilePathStr);
                bw.write("修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldHBFilePathStr: " + oldHBFilePathStr + " newHBFilePathStr: " + newHBFilePathStr);
                updateConfigProcessExit(bw);
            }
        } else {
            updateConfigProcessExit(bw);
        }


        //替换topic
        String oldTopicStr = "agent.sinks.k.kafka.topic";
        String newTopicStr = buildFlumeTopic(flumeDstTopic);
        try {
            FileUtils.modifyFileProperties(flumeConfigFilePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldTopicStr: " + oldTopicStr + " newTopicStr: " + newTopicStr);
            bw.write("修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldTopicStr: " + oldTopicStr + " newTopicStr: " + newTopicStr);
            updateConfigProcessExit(bw);
        }

        //替换kafka地址
        String oldKafkaBrokerStr = "agent.sinks.k.kafka.bootstrap.servers";
        String newKafkaBrokerStr = buildFlumeKafkaBrokerStr(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(flumeConfigFilePath, oldKafkaBrokerStr, newKafkaBrokerStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldKafkaBrokerStr: " + oldKafkaBrokerStr + " newKafkaBrokerStr: " + newKafkaBrokerStr);
            bw.write("修改文件配置出错！flumeBasePath： " + flumeConfigFilePath + " oldKafkaBrokerStr: " + oldKafkaBrokerStr + " newKafkaBrokerStr: " + newKafkaBrokerStr);
            updateConfigProcessExit(bw);
        }

        bw.write("更新flume配置完成！\n");
        System.out.println("更新flume配置成功！\n");
    }

    private void updateConfigProcessExit(BufferedWriter bw) throws IOException {
        bw.write("更新配置失败！\n");
        bw.flush();
        bw.close();
        System.exit(-1);
    }

    private void deployLogstash(LogCheckConfigBean lccb, BufferedWriter bw) throws IOException {
        String kafkaBootstrapServers = lccb.getKafkaBootstrapServers();
        String logstashBasePath = lccb.getLogstashBasePath();
        String logstashExtractFilePath = lccb.getLogstashExtractFilePath();
        String logstashFileStartPosition = lccb.getLogstashFileStartPosition();
        String logstashDstTopic = lccb.getLogstashDstTopic();


        String logstashConfigFilePath = StringUtils.joinWith("/", logstashBasePath, "etc/logstash.conf");
        File logstashConfigFile = new File(logstashConfigFilePath);
        String logstashTemplateFilePath = StringUtils.joinWith("/", logstashBasePath, "etc/logstash.conf.template");
        File logstashTemplateFile = new File(logstashTemplateFilePath);

        //利用文件模板
        //1. 删除所要修改的配置文件
        if (logstashConfigFile.exists())
            FileUtils.deleteFile(logstashConfigFilePath);

        //2. 复制要修改的配置文件
        if (!logstashTemplateFile.exists()) {
            System.out.println("ERROR: [" + logstashConfigFilePath + "] is not exist!");
            bw.write(logstashConfigFilePath + " is not exist!");
            updateConfigProcessExit(bw);
        }

        FileUtils.copyFileUsingFileChannels(logstashTemplateFile, logstashConfigFile);

        if (!logstashConfigFile.exists()) {
            System.out.println("ERROR: [" + logstashConfigFilePath + "] is not exist!");
            bw.write(logstashConfigFilePath + " is not exist!");
            updateConfigProcessExit(bw);
        }


        if (!logstashConfigFile.exists()) {
            System.out.println("ERROR: " + logstashConfigFilePath + " is not exist!");
            bw.write(logstashConfigFilePath + " is not exist!");
            updateConfigProcessExit(bw);
        }

        File logstashExtractFile = new File(logstashExtractFilePath);
        if (FileUtils.checkFileAndFolderIsExist(logstashExtractFilePath)) {
            //修改logstash文件抽取路径
            String oldExtractFilePathStr = "path => [";
            String newExtractFilePathStr = buildLogstashExtractFilePathStr(logstashExtractFilePath);
            try {
                FileUtils.modifyFileProperties(logstashConfigFilePath, oldExtractFilePathStr, newExtractFilePathStr, bw);
            } catch (IOException e) {
                System.out.println("ERROR: 修改文件配置出错！logstashConfigFilePath： " + logstashConfigFilePath + " oldExtractFilePathStr: " + oldExtractFilePathStr + " newExtractFilePathStr: " + newExtractFilePathStr);
                bw.write("修改文件配置出错！logstashConfigFilePath： " + logstashConfigFilePath + " oldExtractFilePathStr: " + oldExtractFilePathStr + " newExtractFilePathStr: " + newExtractFilePathStr);
                updateConfigProcessExit(bw);
            }
        } else {
            updateConfigProcessExit(bw);
        }


        //修改抽取文件是从开始读，还是从末端读，可以为beginning或end
        String oldLogstashFileStartPositionStr = "start_position => ";
        if (!StringUtils.equals(logstashFileStartPosition, "beginning") && !StringUtils.equals(logstashFileStartPosition, "end")) {
            System.out.println("ERROR: " + logstashFileStartPosition + " is error，should be beginning or end!");
            bw.write(logstashFileStartPosition + "  is error，should be beginning or end!");
            bw.newLine();
            updateConfigProcessExit(bw);
        }
        String newLogstashFileStartPositionStr = buildLogstashFileStartPosition(logstashFileStartPosition);
        try {
            FileUtils.modifyFileProperties(logstashConfigFilePath, oldLogstashFileStartPositionStr, newLogstashFileStartPositionStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！logstashConfigFilePath： " + logstashConfigFilePath + " oldLogstashFileStartPositionStr: " + oldLogstashFileStartPositionStr + " newLogstashFileStartPositionStr: " + newLogstashFileStartPositionStr);
            updateConfigProcessExit(bw);
        }

        //修改kafka地址
        String oldkafkaBrokerStr = "bootstrap_servers => ";
        String newkafkaBrokerStr = buildLogstashKafkaBroker(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(logstashConfigFilePath, oldkafkaBrokerStr, newkafkaBrokerStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！logstashConfigFilePath： " + logstashConfigFilePath + " oldkafkaBrokerStr: " + oldkafkaBrokerStr + " newkafkaBrokerStr: " + newkafkaBrokerStr);
            updateConfigProcessExit(bw);
        }
        //修改topic地址
        String oldTopicStr = "topic_id => ";
        String newTopicStr = buildLogstashTopic(logstashDstTopic);
        try {
            FileUtils.modifyFileProperties(logstashConfigFilePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            System.out.println("ERROR: 修改文件配置出错！logstashConfigFilePath： " + logstashConfigFilePath + " oldTopicStr: " + oldTopicStr + " newTopicStr: " + newTopicStr);
            updateConfigProcessExit(bw);
        }

        bw.write("更新Logstash配置成功！\n");
        System.out.println("更新Logstash配置成功！\n");
    }

    private String buildFilebeatFileExtractSection(String filePath) {
        return "\n- type: log\n" +
                "  enabled: true\n" +
                "  paths:\n" +
                "    - " + filePath + "\n" +
                "  fields_under_root: true\n" +
                "  fields:\n" +
                "    type: data_log\n" +
                "  encoding: utf-8\n";
    }

    private String buildFilebeatHBExtractSection(String filePath) {
        return "\n- type: log\n" +
                "  enabled: true\n" +
                "  paths:\n" +
                "    - " + filePath + "\n" +
                "  fields_under_root: true\n" +
                "  fields:\n" +
                "    type: dbus-heartbeat\n" +
                "  encoding: utf-8\n";
    }

    private String buildLogstashHBExtractSection() {
        return "\nheartbeat {\n" +
                "        message => \"epoch\"\n" +
                "        interval => 60\n" +
                "        type => \"dbus-heartbeat\"\n" +
                "    }\n";
    }

    private String buildFilebeatKafkaBrokerConfig(String kafkaBroker) {
        String newHost;
        String[] arr = StringUtils.split(kafkaBroker, ",");
        StringBuffer sb = new StringBuffer("  hosts: [");
        for (int i = 0; i < arr.length; i++) {
            sb.append("\"");
            sb.append(arr[i]);
            sb.append("\"");
            if (i != arr.length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");

        return sb.toString();
    }

    private String buildFilebeatKafkaTopicConfig(String topic) {
        return "  topic: '" + topic + "'";
    }

    private String buildFlumeHeartbeat(String host) {
        return "agent.sources.r_hb_0.interceptors.i_sr_2.replaceString={\\\"message\\\":\\\"$1\\\", \\\"type\\\":\\\"dbus_log\\\", \\\"host\\\":\\\"" + host + "\\\"}";
    }

    private String buildFlumeSincedbStr(String path) {
        return "agent.sources.r_hb_0.positionFile=" + path;
    }

    private String buildFlumeExtractFileStr(String path) {
        return "agent.sources.r_hb_0.filegroups.hblf=" + path + " ";
    }

    private String buildFlumeHBSincedbStr(String path) {
        return "agent.sources.r_dahb.positionFile=" + path;
    }

    private String buildFlumeHBExtractFilePathStr(String path) {
        return "agent.sources.r_dahb.filegroups.dahblf=" + path + " ";
    }

    private String buildFlumeTopic(String topic) {
        return "agent.sinks.k.kafka.topic=" + topic;
    }

    private String buildFlumeKafkaBrokerStr(String kafkaBroker) {
        return "agent.sinks.k.kafka.bootstrap.servers=" + kafkaBroker;
    }

    private String buildLogstashExtractFilePathStr(String path) {
        return "\tpath => [\"" + path + "\"] ";
    }


    private String buildLogstashExtractFileSincedbStr(String path) {
        return "\tsincedb_path => \"" + path + "\"] ";
    }

    private String buildLogstashDsTypeStr(String type) {
        return "type => \"" + type + "\" ";
    }

    private String buildLogstashFileStartPosition(String position) {
        return "\tstart_position => \"" + position + "\" ";
    }

    private String buildLogstashFileFilterType(String type) {
        return "\tif [type] == \"" + type + "\" { ";
    }

    private String buildLogstashKafkaBroker(String kafkaBroker) {
        return "\tbootstrap_servers => \"" + kafkaBroker + "\" ";
    }

    private String buildLogstashTopic(String topic) {
        return "\ttopic_id => \"" + topic + "\" ";
    }

}
