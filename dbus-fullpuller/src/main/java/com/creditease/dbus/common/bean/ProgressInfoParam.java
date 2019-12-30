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

public class ProgressInfoParam {
    private Long partitions;
    private Long totalCount;
    private Long finishedCount;
    private Long totalRows;
    private Long finishedRows;
    private Long startSecs;
    private String createTime;
    private String startTime;
    private String endTime;
    private String errorMsg;
    private String status;
    private String version;
    private String batchNo;
    private String splitStatus;

    public Long getPartitions() {
        return partitions;
    }

    public void setPartitions(Long partitions) {
        this.partitions = partitions;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public Long getFinishedCount() {
        return finishedCount;
    }

    public void setFinishedCount(Long finishedCount) {
        this.finishedCount = finishedCount;
    }

    public Long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Long totalRows) {
        this.totalRows = totalRows;
    }

    public Long getFinishedRows() {
        return finishedRows;
    }

    public void setFinishedRows(Long finishedRows) {
        this.finishedRows = finishedRows;
    }

    public Long getStartSecs() {
        return startSecs;
    }

    public void setStartSecs(Long startSecs) {
        this.startSecs = startSecs;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
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
