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

/**
 * 对应t_table_meta表
 */
public class TableMeta {
    private Integer id;
    private Integer verId;
    private String columnName;
    private String originalColumnName;
    private Integer columnId;
    private Integer internalColumnId;
    private String hiddenColumn;
    private String virtualColumn;
    private Integer originalSer;
    private String dataType;
    private Long dataLength;
    private Integer dataPrecision;
    private Integer dataScale;
    private Integer charLength;
    private String charUsed;
    private String nullable;
    private String isPk;
    private Integer pkPosition;
    private Date alterTime;
    private String comments;
    private String defaultValue;

    private String incompatibleColumn;

    public String getHiddenColumn() {
        return hiddenColumn;
    }

    public void setHiddenColumn(String hiddenColumn) {
        this.hiddenColumn = hiddenColumn;
    }

    public String getVirtualColumn() {
        return virtualColumn;
    }

    public void setVirtualColumn(String virtualColumn) {
        this.virtualColumn = virtualColumn;
    }

    public Integer getCharLength() {
        return charLength;
    }

    public void setCharLength(Integer charLength) {
        this.charLength = charLength;
    }

    public String getCharUsed() {
        return charUsed;
    }

    public void setCharUsed(String charUsed) {
        this.charUsed = charUsed;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getVerId() {
        return verId;
    }

    public void setVerId(Integer verId) {
        this.verId = verId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getOriginalColumnName() {
        return originalColumnName;
    }

    public void setOriginalColumnName(String originalColumnName) {
        this.originalColumnName = originalColumnName;
    }

    public Integer getColumnId() {
        return columnId;
    }

    public void setColumnId(Integer columnId) {
        this.columnId = columnId;
    }

    public Integer getInternalColumnId() {
        return internalColumnId;
    }

    public void setInternalColumnId(Integer internalColumnId) {
        this.internalColumnId = internalColumnId;
    }

    public Integer getOriginalSer() {
        return originalSer;
    }

    public void setOriginalSer(Integer originalSer) {
        this.originalSer = originalSer;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Long getDataLength() {
        return dataLength;
    }

    public void setDataLength(Long dataLength) {
        this.dataLength = dataLength;
    }

    public Integer getDataPrecision() {
        return dataPrecision;
    }

    public void setDataPrecision(Integer dataPrecision) {
        this.dataPrecision = dataPrecision;
    }

    public Integer getDataScale() {
        return dataScale;
    }

    public void setDataScale(Integer dataScale) {
        this.dataScale = dataScale;
    }

    public String getNullable() {
        return nullable;
    }

    public void setNullable(String nullable) {
        this.nullable = nullable;
    }

    public String getIsPk() {
        return isPk;
    }

    public void setIsPk(String isPk) {
        this.isPk = isPk;
    }

    public Integer getPkPosition() {
        return pkPosition;
    }

    public void setPkPosition(Integer pkPosition) {
        this.pkPosition = pkPosition;
    }

    public Date getAlterTime() {
        return alterTime;
    }

    public void setAlterTime(Date alterTime) {
        this.alterTime = alterTime;
    }

    public String getIncompatibleColumn() {
        return incompatibleColumn;
    }

    public void setIncompatibleColumn(String incompatibleColumn) {
        this.incompatibleColumn = incompatibleColumn;
    }
}
