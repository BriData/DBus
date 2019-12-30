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


package com.creditease.dbus.common.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author 201603030496
 * 全量拉取进度信息
 */
@JsonIgnoreProperties({"zkVersion"})
public class ProgressInfo {
    @JsonProperty("Partitions")
    private String partitions;

    @JsonProperty("TotalCount")
    private String totalCount;

    @JsonProperty("FinishedCount")
    private String finishedCount;

    @JsonProperty("TotalRows")
    private String totalRows;

    @JsonProperty("FinishedRows")
    private String finishedRows;

    @JsonProperty("StartSecs")
    private String startSecs;

    @JsonProperty("ConsumeSecs")
    private String consumeSecs;

    @JsonProperty("CreateTime")
    private String createTime;

    @JsonProperty("UpdateTime")
    private String updateTime;

    @JsonProperty("StartTime")
    private String startTime;

    @JsonProperty("EndTime")
    private String endTime;

    @JsonProperty("ErrorMsg")
    private String errorMsg;

    @JsonProperty("Status")
    private String status;


    @JsonProperty("Version")
    private String version;

    @JsonProperty("BatchNo")
    private String batchNo;

    @JsonProperty("SplitStatus")
    private String splitStatus;

    private int zkVersion = -1;

    public int getZkVersion() {
        return zkVersion;
    }

    public void setZkVersion(int zkVersion) {
        this.zkVersion = zkVersion;
    }

    public void mergeProgressInfo(ProgressInfo other) {
        this.errorMsg = other.getErrorMsg();
        this.zkVersion = other.getZkVersion();
    }

    public String getPartitions() {
        return partitions;
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    public String getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(String totalCount) {
        this.totalCount = totalCount;
    }

    public String getFinishedCount() {
        return finishedCount;
    }

    public void setFinishedCount(String finishedCount) {
        this.finishedCount = finishedCount;
    }

    public String getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(String totalRows) {
        this.totalRows = totalRows;
    }

    public String getFinishedRows() {
        return finishedRows;
    }

    public void setFinishedRows(String finishedRows) {
        this.finishedRows = finishedRows;
    }

    public String getStartSecs() {
        return startSecs;
    }

    public void setStartSecs(String startSecs) {
        this.startSecs = startSecs;
    }

    public String getConsumeSecs() {
        return consumeSecs;
    }

    public void setConsumeSecs(String consumeSecs) {
        this.consumeSecs = consumeSecs;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }

    public String getSplitStatus() {
        return splitStatus;
    }

    public void setSplitStatus(String splitStatus) {
        this.splitStatus = splitStatus;
    }
}
