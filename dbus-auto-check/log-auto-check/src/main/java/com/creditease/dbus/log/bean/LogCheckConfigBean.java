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


package com.creditease.dbus.log.bean;

public class LogCheckConfigBean {
    private String filebeatExtractFilePath;
    private String filebeatHeartBeatFilePath;
    private String filebeatBasePath;
    private String filebeatDstTopic;

    private String flumeBasePath;
    private String flumeHost;
    private String flumeDataSincedb;
    private String flumeHeartbeatSincedb;
    private String flumeHeartBeatFilePath;
    private String flumeExtractFilePath;
    private String flumeDstTopic;

    private String logstashBasePath;
    private String logstashExtractFilePath;
    private String logstashSincedbPath;
    private String logstashType;
    private String logstashFileStartPosition;
    private String logstashFilterType;
    private String logstashDstTopic;

    private String kafkaBootstrapServers;
    private String logType;


    public String getFilebeatExtractFilePath() {
        return filebeatExtractFilePath;
    }

    public void setFilebeatExtractFilePath(String filebeatExtractFilePath) {
        this.filebeatExtractFilePath = filebeatExtractFilePath;
    }

    public String getFilebeatBasePath() {
        return filebeatBasePath;
    }

    public void setFilebeatBasePath(String filebeatBasePath) {
        this.filebeatBasePath = filebeatBasePath;
    }

    public String getFilebeatDstTopic() {
        return filebeatDstTopic;
    }

    public void setFilebeatDstTopic(String filebeatDstTopic) {
        this.filebeatDstTopic = filebeatDstTopic;
    }

    public String getFlumeExtractFilePath() {
        return flumeExtractFilePath;
    }

    public void setFlumeExtractFilePath(String flumeExtractFilePath) {
        this.flumeExtractFilePath = flumeExtractFilePath;
    }

    public String getFlumeBasePath() {
        return flumeBasePath;
    }

    public void setFlumeBasePath(String flumeBasePath) {
        this.flumeBasePath = flumeBasePath;
    }

    public String getFlumeDstTopic() {
        return flumeDstTopic;
    }

    public void setFlumeDstTopic(String flumeDstTopic) {
        this.flumeDstTopic = flumeDstTopic;
    }

    public String getLogstashExtractFilePath() {
        return logstashExtractFilePath;
    }

    public void setLogstashExtractFilePath(String logstashExtractFilePath) {
        this.logstashExtractFilePath = logstashExtractFilePath;
    }

    public String getLogstashBasePath() {
        return logstashBasePath;
    }

    public void setLogstashBasePath(String logstashBasePath) {
        this.logstashBasePath = logstashBasePath;
    }

    public String getLogstashDstTopic() {
        return logstashDstTopic;
    }

    public void setLogstashDstTopic(String logstashDstTopic) {
        this.logstashDstTopic = logstashDstTopic;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getFilebeatHeartBeatFilePath() {
        return filebeatHeartBeatFilePath;
    }

    public void setFilebeatHeartBeatFilePath(String filebeatHeartBeatFilePath) {
        this.filebeatHeartBeatFilePath = filebeatHeartBeatFilePath;
    }

    public String getFlumeHeartBeatFilePath() {
        return flumeHeartBeatFilePath;
    }

    public void setFlumeHeartBeatFilePath(String flumeHeartBeatFilePath) {
        this.flumeHeartBeatFilePath = flumeHeartBeatFilePath;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getFlumeHost() {
        return flumeHost;
    }

    public void setFlumeHost(String flumeHost) {
        this.flumeHost = flumeHost;
    }

    public String getFlumeDataSincedb() {
        return flumeDataSincedb;
    }

    public void setFlumeDataSincedb(String flumeDataSincedb) {
        this.flumeDataSincedb = flumeDataSincedb;
    }

    public String getFlumeHeartbeatSincedb() {
        return flumeHeartbeatSincedb;
    }

    public void setFlumeHeartbeatSincedb(String flumeHeartbeatSincedb) {
        this.flumeHeartbeatSincedb = flumeHeartbeatSincedb;
    }

    public String getLogstashSincedbPath() {
        return logstashSincedbPath;
    }

    public void setLogstashSincedbPath(String logstashSincedbPath) {
        this.logstashSincedbPath = logstashSincedbPath;
    }

    public String getLogstashType() {
        return logstashType;
    }

    public void setLogstashType(String logstashType) {
        this.logstashType = logstashType;
    }

    public String getLogstashFileStartPosition() {
        return logstashFileStartPosition;
    }

    public void setLogstashFileStartPosition(String logstashFileStartPosition) {
        this.logstashFileStartPosition = logstashFileStartPosition;
    }

    public String getLogstashFilterType() {
        return logstashFilterType;
    }

    public void setLogstashFilterType(String logstashFilterType) {
        this.logstashFilterType = logstashFilterType;
    }
}
