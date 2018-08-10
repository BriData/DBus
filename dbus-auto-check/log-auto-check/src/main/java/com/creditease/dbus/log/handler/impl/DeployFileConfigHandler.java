package com.creditease.dbus.log.handler.impl;


import com.creditease.dbus.log.bean.LogCheckConfigBean;
import com.creditease.dbus.log.container.AutoCheckConfigContainer;
import com.creditease.dbus.log.handler.AbstractHandler;
import com.creditease.dbus.log.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;


public class DeployFileConfigHandler extends AbstractHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void check(BufferedWriter bw) throws Exception {
    }

    @Override
    public void deploy(BufferedWriter bw) throws Exception {
        LogCheckConfigBean lccb = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String logType = lccb.getLogType();
        if(StringUtils.equals(logType, "filebeat")) {
             deployFilebeat(lccb, bw);
        } else if(StringUtils.equals(logType, "flume")) {
             deployFlume(lccb, bw);
        } else if(StringUtils.equals(logType, "logstash")) {
             deployLogstash(lccb, bw);
        } else {
            bw.write("日志类型错误！请检查配置项!\n");
        }
    }

    private void deployFilebeat(LogCheckConfigBean lccb, BufferedWriter bw) throws IOException {
        String kafkaBootstrapServers = lccb.getKafkaBootstrapServers();
        String filebeatExtractFilePath = lccb.getFilebeatExtractFilePath();
        String filebeatHeartbeatFilePath = lccb.getFilebeatHeartBeatFilePath();
        String filebeatBasePath = lccb.getFilebeatBasePath();
        String dstTopic = lccb.getFilebeatDstTopic();

        String []extractFilePaths = StringUtils.split(filebeatExtractFilePath,",");
        //构造多个抽取路径
        for (int i = 0; i < extractFilePaths.length; i++) {
            String filebeatFileExtractSection = buildFilebeatFileExtractSection(extractFilePaths[i]);
            FileUtils.insert(filebeatBasePath, 22, filebeatFileExtractSection);
        }

        //构造心跳抽取路径
//        String filebeatHBExtractSection = buildFilebeatHBExtractSection(filebeatExtractFilePath);
//        FileUtils.insert(filebeatBasePath, 22, filebeatHBExtractSection);

        //修改kafka地址
        String oldKafkaStr = "hosts: ";
        String newKafkaStr = buildFilebeatKafkaBrokerConfig(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(filebeatBasePath, oldKafkaStr, newKafkaStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！filebeatBasePath： " + filebeatBasePath + "oldKafkaStr: " + oldKafkaStr + "newKafkaStr: " + newKafkaStr);
        }

        //修改topic
        String oldTopicStr = "topic: ";
        String newTopicStr = buildFilebeatKafkaTopicConfig(dstTopic);
        try {
            FileUtils.modifyFileProperties(filebeatBasePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！filebeatBasePath： " + filebeatBasePath + "olTopicStr: " + oldTopicStr + "newTopicStr: " + newTopicStr);
        }

        bw.write("更新filebeat配置成功！\n");
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


        //替换心跳包
        String oldHeartbeatStr = "agent.sources.r_hb_0.interceptors.i_sr_2.replaceString";
        String newHeartbeatStr = buildFlumeHeartbeat(flumeHost);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldHeartbeatStr, newHeartbeatStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + "oldHeartbeatStr: " + oldHeartbeatStr + "newHeartbeatStr: " + newHeartbeatStr);
        }

        //替换数据sincedb
        String oldSincedbStr = "agent.sources.r_hb_0.positionFile";
        String newSincedbStr = buildFlumeSincedbStr(flumeDataSincedb);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldSincedbStr, newSincedbStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + "oldSincedbStr: " + oldSincedbStr + "newSincedbStr: " + newSincedbStr);
        }

        //替换抽取文件路径，flume比较特殊，目前只允许配置一个文件路径，可以是文件夹
        String oldExtractFileStr = "agent.sources.r_hb_0.filegroups.hblf";
        String newExtractFileStr = buildFlumeExtractFileStr(flumeExtractFilePath);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldExtractFileStr, newExtractFileStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + "oldExtractFileStr: " + oldExtractFileStr + "newExtractFileStr: " + newExtractFileStr);
        }

        //替换心跳sincedb
        String oldHBSincedbStr = "agent.sources.r_dahb.positionFile";
        String newHBSincedbStr = buildFlumeSincedbStr(flumeHeartbeatSincedb);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldHBSincedbStr, newHBSincedbStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + " oldHBSincedbStr: " + oldHBSincedbStr + " newHBSincedbStr: " + newHBSincedbStr);
        }

        //替换心跳日志文件路径
        String oldHBFilePathStr = "agent.sources.r_dahb.filegroups.dahblf";
        String newHBFilePathStr = buildFlumeHBExtractFilePathStr(flumeExtractFilePath);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldHBFilePathStr, newHBSincedbStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + " oldHBFilePathStr: " + oldHBFilePathStr + " newHBFilePathStr: " + newHBFilePathStr);
        }

        //替换topic
        String oldTopicStr = "agent.sinks.k.kafka.topic";
        String newTopicStr = buildFlumeTopic(flumeDstTopic);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + " oldTopicStr: " + oldTopicStr + " newTopicStr: " + newTopicStr);
        }

        //替换kafka地址
        String oldKafkaBrokerStr = "agent.sinks.k.kafka.bootstrap.servers";
        String newKafkaBrokerStr = buildFlumeKafkaBrokerStr(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(flumeBasePath, oldKafkaBrokerStr, newKafkaBrokerStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！flumeBasePath： " + flumeBasePath + " oldKafkaBrokerStr: " + oldKafkaBrokerStr + " newKafkaBrokerStr: " + newKafkaBrokerStr);
        }

        bw.write("更新flume配置成功！\n");
    }

    private void deployLogstash(LogCheckConfigBean lccb, BufferedWriter bw) throws IOException {
        String kafkaBootstrapServers = lccb.getKafkaBootstrapServers();
        String logstashBasePath = lccb.getLogstashBasePath();
        String logstashExtractFilePath = lccb.getLogstashExtractFilePath();
        String logstashSincedbPath = lccb.getLogstashSincedbPath();
        String logstashType = lccb.getLogstashType();
        String logstashFileStartPosition = lccb.getLogstashFileStartPosition();
        String logstashFilterType = lccb.getLogstashFilterType();
        String logstashDstTopic = lccb.getLogstashDstTopic();


        //修改logstash文件抽取路径
        String oldExtractFilePathStr = "path => [";
        String newExtractFilePathStr = buildLogstashExtractFilePathStr(logstashExtractFilePath);
        try {
            FileUtils.modifyFileProperties(logstashBasePath, oldExtractFilePathStr, newExtractFilePathStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldExtractFilePathStr: " + oldExtractFilePathStr + " newExtractFilePathStr: " + newExtractFilePathStr);
        }

        //修改抽取文件sincedb存放路径
        String oldExtractFilePathSincedbStr = "sincedb_path => ";
        String newExtractFilePathSincedbStr = buildLogstashExtractFileSincedbStr(logstashSincedbPath);
        try {
            FileUtils.modifyFileProperties(logstashBasePath, oldExtractFilePathSincedbStr, newExtractFilePathSincedbStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldExtractFilePathSincedbStr: " + oldExtractFilePathSincedbStr + " newExtractFilePathSincedbStr: " + newExtractFilePathSincedbStr);
        }

        //修改数据类型，根据该数据类型可对此类数据进行过滤
//        String oldLogstashDsTypeStr = "type => ";
//        String newLogstashDsTypeStr = buildLogstashDsTypeStr(logstashType);
//        try {
//            FileUtils.modifyFileProperties(logstashBasePath, oldLogstashDsTypeStr, newLogstashDsTypeStr, bw);
//        } catch (IOException e) {
//            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldLogstashDsTypeStr: " + oldLogstashDsTypeStr + " newLogstashDsTypeStr: " + newLogstashDsTypeStr);
//        }

        //修改过滤类型，该过滤类型应该要和数据类型一致
//        String oldLogstashFileFilterTypeStr = "start_position => ";
//        String newLogstashFileFilterTypeStr = buildLogstashFileFilterType(logstashFilterType);
//        try {
//            FileUtils.modifyFileProperties(logstashBasePath, oldLogstashFileFilterTypeStr, newLogstashFileFilterTypeStr, bw);
//        } catch (IOException e) {
//            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldLogstashFileFilterTypeStr: " + oldLogstashFileFilterTypeStr + " newLogstashFileFilterTypeStr: " + newLogstashFileFilterTypeStr);
//        }



        //增加logstash心跳组件，必须在修改完数据类型后才能执行该段代码，不可调换顺序
//        String logstashHBExtractSection = buildLogstashHBExtractSection();
//        FileUtils.insert(logstashBasePath, 15, logstashHBExtractSection);


        //修改抽取文件是从开始读，还是从末端读，可以为beginning或end
        String oldLogstashFileStartPositionStr = "start_position => ";
        String newLogstashFileStartPositionStr = buildLogstashFileStartPosition(logstashFileStartPosition);
        try {
            FileUtils.modifyFileProperties(logstashBasePath, oldLogstashFileStartPositionStr, newLogstashFileStartPositionStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldLogstashFileStartPositionStr: " + oldLogstashFileStartPositionStr + " newLogstashFileStartPositionStr: " + newLogstashFileStartPositionStr);
        }


        //修改kafka地址
        String oldkafkaBrokerStr = "bootstrap_servers => ";
        String newkafkaBrokerStr = buildLogstashKafkaBroker(kafkaBootstrapServers);
        try {
            FileUtils.modifyFileProperties(logstashBasePath, oldkafkaBrokerStr, newkafkaBrokerStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldkafkaBrokerStr: " + oldkafkaBrokerStr + " newkafkaBrokerStr: " + newkafkaBrokerStr);
        }
        //修改topic地址
        String oldTopicStr = "topic_id => ";
        String newTopicStr = buildLogstashTopic(logstashDstTopic);
        try {
            FileUtils.modifyFileProperties(logstashBasePath, oldTopicStr, newTopicStr, bw);
        } catch (IOException e) {
            logger.error("修改文件配置出错！logstashBasePath： " + logstashBasePath + " oldTopicStr: " + oldTopicStr + " newTopicStr: " + newTopicStr);
        }

        bw.write("更新Logstash配置成功！\n");
    }

    private String buildFilebeatFileExtractSection(String filePath) {
        return  "\n- type: log\n" +
                "  enabled: true\n" +
                "  paths:\n" +
                "    - "+ filePath + "\n" +
                "  fields_under_root: true\n" +
                "  fields:\n" +
                "    type: data_log\n" +
                "  encoding: utf-8\n";
    }

    private String buildFilebeatHBExtractSection(String filePath) {
        return  "\n- type: log\n" +
                "  enabled: true\n" +
                "  paths:\n" +
                "    - "+ filePath + "\n" +
                "  fields_under_root: true\n" +
                "  fields:\n" +
                "    type: dbus-heartbeat\n" +
                "  encoding: utf-8\n";
    }

    private String buildLogstashHBExtractSection() {
        return  "\nheartbeat {\n" +
                "        message => \"epoch\"\n" +
                "        interval => 60\n" +
                "        type => \"dbus-heartbeat\"\n" +
                "    }\n";
    }

    private String buildFilebeatKafkaBrokerConfig(String kafkaBroker) {
        return  "hosts: [\" "+ kafkaBroker + "\"]";
    }

    private String buildFilebeatKafkaTopicConfig(String topic) {
        return  "topic: '" + topic + "'";
    }

    private String buildFlumeHeartbeat(String host) {
        return  "agent.sources.r_hb_0.interceptors.i_sr_2.replaceString={\\\"message\\\":\\\"$1\\\", \\\"type\\\":\\\"dbus_log\\\", \\\"host\\\":\\\"" + host + "\\\"}\n";
    }

    private String buildFlumeSincedbStr(String path) {
        return  "agent.sources.r_hb_0.positionFile=" + path + " ";
    }

    private  String buildFlumeExtractFileStr(String path) {
        return  "agent.sources.r_hb_0.filegroups.hblf=" + path + " ";
    }

    private  String buildFlumeHBSincedbStr(String path) {
        return  "agent.sources.r_dahb.positionFile=" + path + " ";
    }

    private  String buildFlumeHBExtractFilePathStr(String path) {
        return  "agent.sources.r_dahb.filegroups.dahblf=" + path + " ";
    }

    private  String buildFlumeTopic(String topic) {
        return  "agent.sinks.k.kafka.topic=" + topic + " ";
    }

    private  String buildFlumeKafkaBrokerStr(String kafkaBroker) {
        return  "agent.sinks.k.kafka.bootstrap.servers=" + kafkaBroker + " ";
    }

    private  String buildLogstashExtractFilePathStr(String path) {
        return  "\t\tpath => [\"" + path + "\"] ";
    }


    private  String buildLogstashExtractFileSincedbStr(String path) {
        return  "\t\tsincedb_path => \"" + path + "\"] ";
    }

    private  String buildLogstashDsTypeStr(String type) {
        return  "type => \"" + type + "\" ";
    }

    private  String buildLogstashFileStartPosition(String position) {
        return  "\t\tstart_position => \"" + position + "\" ";
    }

    private  String buildLogstashFileFilterType(String type) {
        return  "\tif [type] == \"" + type + "\" { ";
    }

    private  String buildLogstashKafkaBroker(String kafkaBroker) {
        return  "\t\tbootstrap_servers => \"" + kafkaBroker + "\" ";
    }

    private  String buildLogstashTopic(String topic) {
        return  "\t\ttopic_id => \"" + topic + "\" ";
    }




}
