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
 * Created by Administrator on 2018/6/5.
 */
public class Stat implements Serializable {

    private String dsName;

    private String schemaName;

    private String tableName;

    private Long time;

    private Long txTime;

    private Integer successCnt = 0;

    private Integer errorCnt = 0;

    private Integer warnCnt = 0;

    private Integer taskId = 0;

    private Integer taskIdSum = 0;

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

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getTxTime() {
        return txTime;
    }

    public void setTxTime(Long txTime) {
        this.txTime = txTime;
    }

    public Integer getSuccessCnt() {
        return successCnt;
    }

    public void setSuccessCnt(Integer successCnt) {
        this.successCnt = successCnt;
    }

    public Integer getErrorCnt() {
        return errorCnt;
    }

    public void setErrorCnt(Integer errorCnt) {
        this.errorCnt = errorCnt;
    }

    public Integer getWarnCnt() {
        return warnCnt;
    }

    public void setWarnCnt(Integer warnCnt) {
        this.warnCnt = warnCnt;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getTaskIdSum() {
        return taskIdSum;
    }

    public void setTaskIdSum(Integer taskIdSum) {
        this.taskIdSum = taskIdSum;
    }

    public void merge(Stat vo, boolean isUseBarrier) {
        this.setDsName(vo.getDsName());
        this.setSchemaName(vo.getSchemaName());
        this.setTableName(vo.getTableName());
        this.setTaskId(vo.getTaskId());
        this.successCnt += vo.getSuccessCnt();
        this.errorCnt += vo.getErrorCnt();
        this.warnCnt += vo.getWarnCnt();
        if (isUseBarrier)
            this.taskIdSum += vo.taskIdSum;
    }

    public void correc(Integer size) {
        if (this.successCnt >= size) this.successCnt -= size;
        this.errorCnt += size;
    }

}
