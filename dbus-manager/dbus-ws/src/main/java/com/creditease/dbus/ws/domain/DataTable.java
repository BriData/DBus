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

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.Date;

public class DataTable implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 表单tables状态,ok状态
     */
    public static final String OK = "ok";
    /**
     * 表单tables状态,abort状态
     */
    public static final String ABORT = "abort";

    /**
     * 表单tables状态,waiting状态
     */
    public static final String WAITING = "waiting";

    private Long id;

    private Long dsID;

    private String dsName;

    private String dsType;

    private Long schemaID;

    private String schemaName;

    private String tableName;

    private String physicalTableRegex;

    private String outputTopic;

    private Long verID;

    private Long version;

    private Long innerVersion;

    private String status;

    private Long metaChangeFlg;

    private String masterUrl;

    private String slaveUrl;

    private String dbusUser;

    private String dbusPassword;

    //这个是版本的id历史
    private String verChangeHistory;

    //这个是真正的版本号历史，直接通过版本id在webservice中转换，数据库中无此项
    private String versionsChangeHistory;

    private int verChangeNoticeFlg;

    public int getOutputBeforeUpdateFlg() {
        return outputBeforeUpdateFlg;
    }

    public void setOutputBeforeUpdateFlg(int outputBeforeUpdateFlg) {
        this.outputBeforeUpdateFlg = outputBeforeUpdateFlg;
    }

    private int outputBeforeUpdateFlg;

    public String getVersionsChangeHistory() {
        return versionsChangeHistory;
    }

    public void setVersionsChangeHistory(String versionsChangeHistory) {
        this.versionsChangeHistory = versionsChangeHistory;
    }

    public String getVerChangeHistory() {
        return verChangeHistory;
    }

    public void setVerChangeHistory(String verChangeHistory) {
        this.verChangeHistory = verChangeHistory;
    }

    public int getVerChangeNoticeFlg() {
        return verChangeNoticeFlg;
    }

    public void setVerChangeNoticeFlg(int verChangeNoticeFlg) {
        this.verChangeNoticeFlg = verChangeNoticeFlg;
    }

    public Long getMetaChangeFlg() {
        return metaChangeFlg;
    }

    public void setMetaChangeFlg(Long metaChangeFlg) {
        this.metaChangeFlg = metaChangeFlg;
    }

    public String getDbusPassword() {
        return dbusPassword;
    }

    public void setDbusPassword(String dbusPassword) {
        this.dbusPassword = dbusPassword;
    }

    public String getSlaveUrl() {
        return slaveUrl;
    }

    public void setSlaveUrl(String slaveUrl) {
        this.slaveUrl = slaveUrl;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getDbusUser() {
        return dbusUser;
    }

    public void setDbusUser(String dbusUser) {
        this.dbusUser = dbusUser;
    }

    @JSONField(format = "yyyy-MM-dd HH:mm:ss SSS")
    private Date createTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDsID() {
        return dsID;
    }

    public void setDsID(Long dsID) {
        this.dsID = dsID;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public Long getSchemaID() {
        return schemaID;
    }

    public void setSchemaID(Long schemaID) {this.schemaID = schemaID;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhysicalTableRegex() {
        return physicalTableRegex;
    }

    public void setPhysicalTableRegex(String physicalTableRegex) {
        this.physicalTableRegex = physicalTableRegex;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public Long getVerID() {
        return verID;
    }

    public void setVerID(Long verID) {
        this.verID = verID;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Long getInnerVersion() {
        return innerVersion;
    }

    public void setInnerVersion(Long innerVersion) {
        this.innerVersion = innerVersion;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getDsType() { return dsType;}

    public void setDsType(String dsType) { this.dsType = dsType;}

}
