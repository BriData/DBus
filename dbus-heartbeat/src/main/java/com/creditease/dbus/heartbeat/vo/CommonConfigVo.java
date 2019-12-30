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


package com.creditease.dbus.heartbeat.vo;

public class CommonConfigVo {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1319087951603432207L;

    private String sinkerCheckAlarmInterval;
    private String sinkerHeartbeatTopic;
    private String sinkerKafkaOffset;
    private String flowLineCheckUrl;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getSinkerCheckAlarmInterval() {
        return sinkerCheckAlarmInterval;
    }

    public void setSinkerCheckAlarmInterval(String sinkerCheckAlarmInterval) {
        this.sinkerCheckAlarmInterval = sinkerCheckAlarmInterval;
    }

    public String getSinkerHeartbeatTopic() {
        return sinkerHeartbeatTopic;
    }

    public void setSinkerHeartbeatTopic(String sinkerHeartbeatTopic) {
        this.sinkerHeartbeatTopic = sinkerHeartbeatTopic;
    }

    public String getSinkerKafkaOffset() {
        return sinkerKafkaOffset;
    }

    public void setSinkerKafkaOffset(String sinkerKafkaOffset) {
        this.sinkerKafkaOffset = sinkerKafkaOffset;
    }

    public String getFlowLineCheckUrl() {
        return flowLineCheckUrl;
    }

    public void setFlowLineCheckUrl(String flowLineCheckUrl) {
        this.flowLineCheckUrl = flowLineCheckUrl;
    }
}
