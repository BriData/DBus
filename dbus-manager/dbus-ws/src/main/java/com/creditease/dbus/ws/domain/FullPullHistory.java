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

import java.io.Serializable;
import java.util.Date;

public class FullPullHistory implements Serializable {
    private static final long serialVersionUID = 1L;

    private long id;
    private String type;
    private String dsName;
    private String schemaName;
    private String tableName;
    private int version;
    private int batchId;
    private String state;
    private String errorMsg;
    private Date initTime;
    private Date startSplitTime;
    private Date startPullTime;
    private Date endTime;
    private Date updateTime;
    private long finishedPartitionCount;
    private long totalPartitionCount;
    private long finishedRowCount;
    private long totalRowCount;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getBatchId() {
        return batchId;
    }

    public void setBatchId(int batchId) {
        this.batchId = batchId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Date getInitTime() {
        return initTime;
    }

    public void setInitTime(Date initTime) {
        this.initTime = initTime;
    }

    public Date getStartSplitTime() {
        return startSplitTime;
    }

    public void setStartSplitTime(Date startSplitTime) {
        this.startSplitTime = startSplitTime;
    }

    public Date getStartPullTime() {
        return startPullTime;
    }

    public void setStartPullTime(Date startPullTime) {
        this.startPullTime = startPullTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public long getFinishedPartitionCount() {
        return finishedPartitionCount;
    }

    public void setFinishedPartitionCount(long finishedPartitionCount) {
        this.finishedPartitionCount = finishedPartitionCount;
    }

    public long getTotalPartitionCount() {
        return totalPartitionCount;
    }

    public void setTotalPartitionCount(long totalPartitionCount) {
        this.totalPartitionCount = totalPartitionCount;
    }

    public long getFinishedRowCount() {
        return finishedRowCount;
    }

    public void setFinishedRowCount(long finishedRowCount) {
        this.finishedRowCount = finishedRowCount;
    }

    public long getTotalRowCount() {
        return totalRowCount;
    }

    public void setTotalRowCount(long totalRowCount) {
        this.totalRowCount = totalRowCount;
    }
}
