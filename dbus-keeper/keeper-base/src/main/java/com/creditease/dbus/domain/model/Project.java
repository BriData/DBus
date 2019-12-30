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


package com.creditease.dbus.domain.model;

import java.util.Date;

public class Project {
    private Integer id;

    private String projectName;

    private String projectDisplayName;

    private String projectOwner;

    private String projectIcon;

    private String projectDesc;

    private Date projectExpire;

    private Byte topologyNum;

    private Byte allowAdminManage;

    private Byte schemaChangeNotifyFlag;

    private String schemaChangeNotifyEmails;

    private Byte slaveSyncDelayNotifyFlag;

    private String slaveSyncDelayNotifyEmails;

    private Byte fullpullNotifyFlag;

    private String fullpullNotifyEmails;

    private Byte dataDelayNotifyFlag;

    private String dataDelayNotifyEmails;

    private String status;

    private String stormStartPath;

    private String stormSshUser;

    private String stormApiUrl;

    private Date updateTime;

    private String keytabPath;

    private String principal;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getProjectDisplayName() {
        return projectDisplayName;
    }

    public void setProjectDisplayName(String projectDisplayName) {
        this.projectDisplayName = projectDisplayName;
    }

    public String getProjectOwner() {
        return projectOwner;
    }

    public void setProjectOwner(String projectOwner) {
        this.projectOwner = projectOwner;
    }

    public String getProjectIcon() {
        return projectIcon;
    }

    public void setProjectIcon(String projectIcon) {
        this.projectIcon = projectIcon;
    }

    public String getProjectDesc() {
        return projectDesc;
    }

    public void setProjectDesc(String projectDesc) {
        this.projectDesc = projectDesc;
    }

    public Date getProjectExpire() {
        return projectExpire;
    }

    public void setProjectExpire(Date projectExpire) {
        this.projectExpire = projectExpire;
    }

    public Byte getTopologyNum() {
        return topologyNum;
    }

    public void setTopologyNum(Byte topologyNum) {
        this.topologyNum = topologyNum;
    }

    public Byte getAllowAdminManage() {
        return allowAdminManage;
    }

    public void setAllowAdminManage(Byte allowAdminManage) {
        this.allowAdminManage = allowAdminManage;
    }

    public Byte getSchemaChangeNotifyFlag() {
        return schemaChangeNotifyFlag;
    }

    public void setSchemaChangeNotifyFlag(Byte schemaChangeNotifyFlag) {
        this.schemaChangeNotifyFlag = schemaChangeNotifyFlag;
    }

    public String getSchemaChangeNotifyEmails() {
        return schemaChangeNotifyEmails;
    }

    public void setSchemaChangeNotifyEmails(String schemaChangeNotifyEmails) {
        this.schemaChangeNotifyEmails = schemaChangeNotifyEmails;
    }

    public Byte getSlaveSyncDelayNotifyFlag() {
        return slaveSyncDelayNotifyFlag;
    }

    public void setSlaveSyncDelayNotifyFlag(Byte slaveSyncDelayNotifyFlag) {
        this.slaveSyncDelayNotifyFlag = slaveSyncDelayNotifyFlag;
    }

    public String getSlaveSyncDelayNotifyEmails() {
        return slaveSyncDelayNotifyEmails;
    }

    public void setSlaveSyncDelayNotifyEmails(String slaveSyncDelayNotifyEmails) {
        this.slaveSyncDelayNotifyEmails = slaveSyncDelayNotifyEmails;
    }

    public Byte getFullpullNotifyFlag() {
        return fullpullNotifyFlag;
    }

    public void setFullpullNotifyFlag(Byte fullpullNotifyFlag) {
        this.fullpullNotifyFlag = fullpullNotifyFlag;
    }

    public String getFullpullNotifyEmails() {
        return fullpullNotifyEmails;
    }

    public void setFullpullNotifyEmails(String fullpullNotifyEmails) {
        this.fullpullNotifyEmails = fullpullNotifyEmails;
    }

    public Byte getDataDelayNotifyFlag() {
        return dataDelayNotifyFlag;
    }

    public void setDataDelayNotifyFlag(Byte dataDelayNotifyFlag) {
        this.dataDelayNotifyFlag = dataDelayNotifyFlag;
    }

    public String getDataDelayNotifyEmails() {
        return dataDelayNotifyEmails;
    }

    public void setDataDelayNotifyEmails(String dataDelayNotifyEmails) {
        this.dataDelayNotifyEmails = dataDelayNotifyEmails;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStormStartPath() {
        return stormStartPath;
    }

    public void setStormStartPath(String stormStartPath) {
        this.stormStartPath = stormStartPath;
    }

    public String getStormSshUser() {
        return stormSshUser;
    }

    public void setStormSshUser(String stormSshUser) {
        this.stormSshUser = stormSshUser;
    }

    public String getStormApiUrl() {
        return stormApiUrl;
    }

    public void setStormApiUrl(String stormApiUrl) {
        this.stormApiUrl = stormApiUrl;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }
}
