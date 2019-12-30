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


package com.creditease.dbus.log.processor.window;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by zhenlinzhong on 2017/11/15.
 */
public class Element {

    private Boolean isOk = false;

    private String table;

    private Long timestamp;

    private Integer taskId;

    private Integer taskIndex;

    private Integer taskIdSum = 0;

    private Integer taskIndexSum = 0;

    private String namespace;

    private String host;

    private String umsSource;


    public Boolean getOk() {
        return isOk;
    }

    public void setOk(Boolean ok) {
        isOk = ok;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(Integer taskIndex) {
        this.taskIndex = taskIndex;
    }

    public Integer getTaskIdSum() {
        return taskIdSum;
    }

    public void setTaskIdSum(Integer taskIdSum) {
        this.taskIdSum = taskIdSum;
    }

    public Integer getTaskIndexSum() {
        return taskIndexSum;
    }

    public void setTaskIndexSum(Integer taskIndexSum) {
        this.taskIndexSum = taskIndexSum;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKey() {
        return StringUtils.joinWith("_", host, table, timestamp);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUmsSource() {
        return umsSource;
    }

    public void setUmsSource(String umsSource) {
        this.umsSource = umsSource;
    }

    public void merge(Element e, Integer taskIdSum) {
        this.taskIdSum += e.getTaskId();
        this.taskIndexSum += e.getTaskIndex();
        this.isOk = (this.taskIdSum >= taskIdSum);
    }

    public void merge(Integer taskIdSum) {
        this.taskIdSum = this.getTaskId();
        this.taskIndexSum = this.getTaskIndex();
        this.isOk = (this.taskIdSum >= taskIdSum);
    }

}
