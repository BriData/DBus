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
import java.util.List;

/**
 * Created by Shrimp on 16/9/8.
 */
public class TableVersion {
    private Integer id;
    private Integer tableId;
    private Integer dsId;
    private String dbName;
    private String schemaName;
    private String tableName;
    private Integer version;
    private Integer innerVersion;
    private Long eventOffset;
    private Long eventPos;
    private Date updateTime;
    private String comments;

    private List<TableVersion> tableVersions;

    public List<TableVersion> getTableVersions() {
        return tableVersions;
    }

    public void setTableVersions(List<TableVersion> tableVersions) {
        this.tableVersions = tableVersions;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public Integer getDsId() {
        return dsId;
    }

    public void setDsId(Integer dsId) {
        this.dsId = dsId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public Long getEventOffset() {
        return eventOffset;
    }

    public void setEventOffset(Long eventOffset) {
        this.eventOffset = eventOffset;
    }

    public Long getEventPos() {
        return eventPos;
    }

    public void setEventPos(Long eventPos) {
        this.eventPos = eventPos;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

}
