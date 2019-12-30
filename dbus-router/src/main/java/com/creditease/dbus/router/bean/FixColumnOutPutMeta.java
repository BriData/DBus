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


package com.creditease.dbus.router.bean;

import java.io.Serializable;

/**
 * Created by mal on 2018/5/29.
 */
public class FixColumnOutPutMeta implements Serializable {

    private Long projectId;

    private Long tableId;

    private Long version;

    private String columnName;

    private String dataType;

    private Integer schemaChangeFlag;

    private String schemaChangeComment;

    private Long tpttmvId;

    private int precision;

    private int scale;

    private int length;

    private Long tpttId;

    private boolean isExist = false;

    private boolean isChanged = false;

    private boolean isDelete = false;

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Integer getSchemaChangeFlag() {
        return schemaChangeFlag;
    }

    public void setSchemaChangeFlag(Integer schemaChangeFlag) {
        this.schemaChangeFlag = schemaChangeFlag;
    }

    public Long getTpttmvId() {
        return tpttmvId;
    }

    public void setTpttmvId(Long tpttmvId) {
        this.tpttmvId = tpttmvId;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isExist() {
        return isExist;
    }

    public void setExist(boolean exist) {
        isExist = exist;
    }

    public boolean isChanged() {
        return isChanged;
    }

    public void setChanged(boolean changed) {
        isChanged = changed;
    }

    public Long getTpttId() {
        return tpttId;
    }

    public void setTpttId(Long tpttId) {
        this.tpttId = tpttId;
    }

    public String getSchemaChangeComment() {
        return schemaChangeComment;
    }

    public void setSchemaChangeComment(String schemaChangeComment) {
        this.schemaChangeComment = schemaChangeComment;
    }

    public boolean isDelete() {
        return isDelete;
    }

    public void setDelete(boolean delete) {
        isDelete = delete;
    }
}
