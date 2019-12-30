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

public class ProjectEncodeHint {
    private Integer id;

    private String desc;

    private Integer projectId;

    private Integer tableId;

    private String fieldName;

    private Integer encodePluginId;

    private String encodeType;

    private String encodeParam;

    private Integer truncate;

    private Date updateTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Integer getProjectId() {
        return projectId;
    }

    public void setProjectId(Integer projectId) {
        this.projectId = projectId;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getEncodeType() {
        return encodeType;
    }

    public void setEncodeType(String encodeType) {
        this.encodeType = encodeType;
    }

    public String getEncodeParam() {
        return encodeParam;
    }

    public void setEncodeParam(String encodeParam) {
        this.encodeParam = encodeParam;
    }

    public Integer getTruncate() {
        return truncate;
    }

    public void setTruncate(Integer truncate) {
        this.truncate = truncate;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Integer getEncodePluginId() {
        return encodePluginId;
    }

    public void setEncodePluginId(Integer encodePluginId) {
        this.encodePluginId = encodePluginId;
    }
}
