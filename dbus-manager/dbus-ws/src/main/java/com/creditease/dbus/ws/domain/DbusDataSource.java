/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.domain;

import java.io.Serializable;
import java.util.Date;


/**
 * Created by dongwang47 on 2016/8/29.
 */
public class DbusDataSource implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 数据源状态,有效状态
     */
    public static final String ACTIVE = "active";
    /**
     * 数据源状态,无效状态
     */
    public static final String INACTIVE = "inactive";

    private Long id;

    private String dsName;

    private String dsType;

    private String status;

    private String dsDesc;

    private String topic;

    private String ctrlTopic;

    private String schemaTopic;

    private String splitTopic;

    private String masterURL;

    private String slaveURL;

    private String dbusUser;

    private String dbusPassword;

    private Date updateTime;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDsDesc() {
        return dsDesc;
    }

    public void setDsDesc(String dsDesc) {
        this.dsDesc = dsDesc;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCtrlTopic() {
        return ctrlTopic;
    }

    public void setCtrlTopic(String ctrlTopic) {
        this.ctrlTopic = ctrlTopic;
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

    public String getMasterURL() {
        return masterURL;
    }

    public void setMasterURL(String masterURL) {
        this.masterURL = masterURL;
    }

    public String getSlaveURL() {
        return slaveURL;
    }

    public void setSlaveURL(String slaveURL) {
        this.slaveURL = slaveURL;
    }

    public String getDbusUser() {
        return dbusUser;
    }

    public void setDbusUser(String dbusUser) {
        this.dbusUser = dbusUser;
    }

    public String getDbusPassword() {
        return dbusPassword;
    }

    public void setDbusPassword(String dbusPassword) {
        this.dbusPassword = dbusPassword;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
