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

public class ProjectTopoTableEncodeOutputColumns {
    private Integer id;

    private Integer projectTopoTableId;

    private String fieldName;

    private Integer encodePluginId;

    private String fieldType;

    private Integer dataLength;

    private String encodeType;

    private String encodeParam;

    private String desc;

    private Integer truncate;

    private Integer encodeSource;

    private Date updateTime;

    private Byte schemaChangeFlag;

    private Integer dataScale;

    private Integer dataPrecision;

    private String schemaChangeComment;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getProjectTopoTableId() {
        return projectTopoTableId;
    }

    public void setProjectTopoTableId(Integer projectTopoTableId) {
        this.projectTopoTableId = projectTopoTableId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getEncodePluginId() {
        return encodePluginId;
    }

    public void setEncodePluginId(Integer encodePluginId) {
        this.encodePluginId = encodePluginId;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public Integer getDataLength() {
        return dataLength;
    }

    public void setDataLength(Integer dataLength) {
        this.dataLength = dataLength;
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

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Integer getTruncate() {
        return truncate;
    }

    public void setTruncate(Integer truncate) {
        this.truncate = truncate;
    }

    public Integer getEncodeSource() {
        return encodeSource;
    }

    public void setEncodeSource(Integer encodeSource) {
        this.encodeSource = encodeSource;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Byte getSchemaChangeFlag() {
        return schemaChangeFlag;
    }

    public void setSchemaChangeFlag(Byte schemaChangeFlag) {
        this.schemaChangeFlag = schemaChangeFlag;
    }

    public Integer getDataScale() {
        return dataScale;
    }

    public void setDataScale(Integer dataScale) {
        this.dataScale = dataScale;
    }

    public Integer getDataPrecision() {
        return dataPrecision;
    }

    public void setDataPrecision(Integer dataPrecision) {
        this.dataPrecision = dataPrecision;
    }

    public String getSchemaChangeComment() {
        return schemaChangeComment;
    }

    public void setSchemaChangeComment(String schemaChangeComment) {
        this.schemaChangeComment = schemaChangeComment;
    }
}
