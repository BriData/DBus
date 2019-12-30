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


package com.creditease.dbus.bean;

import com.creditease.dbus.domain.model.TableMeta;
import com.creditease.dbus.domain.model.TableVersion;

import java.util.Date;
import java.util.List;

public class DataTableBean {

    private Integer id;

    private Integer dsId;

    private Integer schemaId;

    private String schemaName;

    private String tableName;

    private String tableNameAlias;

    private String physicalTableRegex;

    private String outputTopic;

    private Integer verId;

    private String status;

    private Date createTime;

    private Integer metaChangeFlg;

    private Integer batchId;

    private String verChangeHistory;

    private Integer verChangeNoticeFlg;

    private Integer outputBeforeUpdateFlg;

    private String description;

    //以下是t_data_source的相关列
    private String dsName;

    private String dsType;

    private String ctrlTopic;

    private String masterUrl;

    private String slaveUrl;

    private String dbusUser;

    private String dbusPassword;

    //以下是t_meta_version的相关列
    private Integer version;

    private Integer innerVersion;

    private String uniqueColumn;

    private String namespace;

    private List<TableVersion> versionList;

    private List<TableMeta> tableMetaList;

    private String oriMasterUrl;

    private String oriSlaveUrl;

    public String getOriMasterUrl() {
        return oriMasterUrl;
    }

    public void setOriMasterUrl(String oriMasterUrl) {
        this.oriMasterUrl = oriMasterUrl;
    }

    public String getOriSlaveUrl() {
        return oriSlaveUrl;
    }

    public void setOriSlaveUrl(String oriSlaveUrl) {
        this.oriSlaveUrl = oriSlaveUrl;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getDsId() {
        return dsId;
    }

    public void setDsId(Integer dsId) {
        this.dsId = dsId;
    }

    public Integer getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Integer schemaId) {
        this.schemaId = schemaId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableNameAlias() {
        return tableNameAlias;
    }

    public void setTableNameAlias(String tableNameAlias) {
        this.tableNameAlias = tableNameAlias;
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

    public Integer getVerId() {
        return verId;
    }

    public void setVerId(Integer verId) {
        this.verId = verId;
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

    public Integer getMetaChangeFlg() {
        return metaChangeFlg;
    }

    public void setMetaChangeFlg(Integer metaChangeFlg) {
        this.metaChangeFlg = metaChangeFlg;
    }

    public Integer getBatchId() {
        return batchId;
    }

    public void setBatchId(Integer batchId) {
        this.batchId = batchId;
    }

    public String getVerChangeHistory() {
        return verChangeHistory;
    }

    public void setVerChangeHistory(String verChangeHistory) {
        this.verChangeHistory = verChangeHistory;
    }

    public Integer getVerChangeNoticeFlg() {
        return verChangeNoticeFlg;
    }

    public void setVerChangeNoticeFlg(Integer verChangeNoticeFlg) {
        this.verChangeNoticeFlg = verChangeNoticeFlg;
    }

    public Integer getOutputBeforeUpdateFlg() {
        return outputBeforeUpdateFlg;
    }

    public void setOutputBeforeUpdateFlg(Integer outputBeforeUpdateFlg) {
        this.outputBeforeUpdateFlg = outputBeforeUpdateFlg;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public String getCtrlTopic() {
        return ctrlTopic;
    }

    public void setCtrlTopic(String ctrlTopic) {
        this.ctrlTopic = ctrlTopic;
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

    public String getDbusPassword() {
        return dbusPassword;
    }

    public void setDbusPassword(String dbusPassword) {
        this.dbusPassword = dbusPassword;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getInnerVersion() {
        return innerVersion;
    }

    public void setInnerVersion(Integer innerVersion) {
        this.innerVersion = innerVersion;
    }

    public String getUniqueColumn() {
        return uniqueColumn;
    }

    public void setUniqueColumn(String uniqueColumn) {
        this.uniqueColumn = uniqueColumn;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public List<TableVersion> getVersionList() {
        return versionList;
    }

    public void setVersionList(List<TableVersion> versionList) {
        this.versionList = versionList;
    }

    public List<TableMeta> getTableMetaList() {
        return tableMetaList;
    }

    public void setTableMetaList(List<TableMeta> tableMetaList) {
        this.tableMetaList = tableMetaList;
    }
}
