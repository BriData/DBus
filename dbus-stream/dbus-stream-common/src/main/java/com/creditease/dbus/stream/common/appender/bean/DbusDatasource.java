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


package com.creditease.dbus.stream.common.appender.bean;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by Shrimp on 16/5/26.
 */
public class DbusDatasource implements Serializable {
    private long id;
    private String dsName;
    private String dsType;
    private String dsDesc;
    private String masterUrl;
    private String slaveUrl;
    private Timestamp ts;
    private String dbusUser;
    private String dbusPwd;
    private String topic;
    private String controlTopic;
    private String schemaTopic;
    private String splitTopic;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
    }

    public String getDsDesc() {
        return dsDesc;
    }

    public void setDsDesc(String dsDesc) {
        this.dsDesc = dsDesc;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getSlaveUrl() {
        return slaveUrl;
    }

    public void setSlaveUrl(String slaveUrl) {
        this.slaveUrl = slaveUrl;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getDbusUser() {
        return dbusUser;
    }

    public void setDbusUser(String dbusUser) {
        this.dbusUser = dbusUser;
    }

    public String getDbusPwd() {
        return dbusPwd;
    }

    public void setDbusPwd(String dbusPwd) {
        this.dbusPwd = dbusPwd;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getControlTopic() {
        return controlTopic;
    }

    public void setControlTopic(String controlTopic) {
        this.controlTopic = controlTopic;
    }

    public String getSchemaTopic() {
        return schemaTopic;
    }

    public void setSchemaTopic(String schemaTopic) {
        this.schemaTopic = schemaTopic;
    }

    public String getSplitTopic() {
        return splitTopic;
    }

    public void setSplitTopic(String splitTopic) {
        this.splitTopic = splitTopic;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
