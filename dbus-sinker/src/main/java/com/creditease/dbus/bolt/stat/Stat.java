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


package com.creditease.dbus.bolt.stat;

public class Stat {
    private String dsName;
    private String schemaName;
    private String tableName;
    private Integer successCnt = 0;
    private Integer errorCnt = 0;
    private Integer warnCnt = 0;

    public Stat(String dsName, String schemaName, String tableName) {
        this.dsName = dsName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public void commitDataCount(Integer count) {
        this.successCnt += count;
    }

    public void failDataCount(Integer count) {
        this.successCnt = (successCnt -= count) > 0 ? successCnt -= count : 0;
        this.errorCnt += count;
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

    //发送到kafka成功,计数清零
    public void success() {
        this.successCnt = 0;
        this.errorCnt = 0;
        this.warnCnt = 0;
    }

    public void fail() {
    }
}
