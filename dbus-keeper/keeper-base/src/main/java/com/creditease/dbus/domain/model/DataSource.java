/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.domain.model;

import java.util.Date;

public class DataSource {
    /**
     * 数据源状态,有效状态
     */
    public static final String ACTIVE = "active";
    /**
     * 数据源状态,无效状态
     */
    public static final String INACTIVE = "inactive";

    public static final String KEY_NAME = "name";
    public static final String KEY_TYPE = "type";

    private Integer id;

    private String dsName;

    private String dsType;

    private String instanceName;

    private String status;

    private String dsDesc;

    private String topic;

    private String ctrlTopic;

    private String schemaTopic;

    private String splitTopic;

    private String masterUrl;

    private String slaveUrl;

    private String dbusUser;

    private String dbusPwd;

    private Date updateTime;

    private String dsPartition;

    private String canalUser;

    private String canalPass;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
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

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
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

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getDsPartition() {
        return dsPartition;
    }

    public void setDsPartition(String dsPartition) {
        this.dsPartition = dsPartition;
    }

    public String getCanalUser() {
        return canalUser;
    }

    public void setCanalUser(String canalUser) {
        this.canalUser = canalUser;
    }

    public String getCanalPass() {
        return canalPass;
    }

    public void setCanalPass(String canalPass) {
        this.canalPass = canalPass;
    }
}
