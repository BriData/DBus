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

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by haowei6 on 2017/9/14.
 */
public class DdlEvent implements Serializable {
    private long id;
    private long eventId;
    private long dsId;
    private String schemaName;
    private String tableName;
    private String columnName;
    private long verId;
    private String triggerVer;
    private String ddlType;
    private String ddl;
    private Timestamp updateTime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public long getDsId() {
        return dsId;
    }

    public void setDsId(long dsId) {
        this.dsId = dsId;
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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public long getVerId() {
        return verId;
    }

    public void setVerId(long verId) {
        this.verId = verId;
    }

    public String getTriggerVer() {
        return triggerVer;
    }

    public void setTriggerVer(String triggerVer) {
        this.triggerVer = triggerVer;
    }

    public String getDdlType() {
        return ddlType;
    }

    public void setDdlType(String ddlType) {
        this.ddlType = ddlType;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}
