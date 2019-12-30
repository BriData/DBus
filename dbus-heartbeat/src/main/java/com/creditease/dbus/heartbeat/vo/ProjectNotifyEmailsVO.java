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

/**
 * User: 王少楠
 * Date: 2018-07-17
 * Desc: project报警策略的通知邮件地址
 */
public class ProjectNotifyEmailsVO {
    /**
     * 表结构变更邮件通知地址
     */
    private String[] schemaChangeEmails;

    /**
     * 全量报警邮件通知地址
     */
    private String[] fullPullerEmails;

    /**
     * Topology延时报警邮件通知地址
     */
    private String[] topologyDelayEmails;

    /**
     * 主备延时报警邮件通知地址
     */
    private String[] masterSlaveDelayEmails;

    public String[] getSchemaChangeEmails() {
        return schemaChangeEmails;
    }

    public void setSchemaChangeEmails(String[] schemaChangeEmails) {
        this.schemaChangeEmails = schemaChangeEmails;
    }

    public String[] getFullPullerEmails() {
        return fullPullerEmails;
    }

    public void setFullPullerEmails(String[] fullPullerEmails) {
        this.fullPullerEmails = fullPullerEmails;
    }

    public String[] getTopologyDelayEmails() {
        return topologyDelayEmails;
    }

    public void setTopologyDelayEmails(String[] topologyDelayEmails) {
        this.topologyDelayEmails = topologyDelayEmails;
    }

    public String[] getMasterSlaveDelayEmails() {
        return masterSlaveDelayEmails;
    }

    public void setMasterSlaveDelayEmails(String[] masterSlaveDelayEmails) {
        this.masterSlaveDelayEmails = masterSlaveDelayEmails;
    }
}
