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


package com.creditease.dbus.log.processor.vo;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/10/12.
 */
public class RuleInfo implements Serializable {

    private String dsName;
    private String schemaName;
    private Long tableId;
    private String tableName;
    private String outputTopic;
    private Long groupId;
    private String groupName;
    private String status;
    private Integer orderId;
    private String ruleTypeName;
    private String ruleGrammar;
    private Long version;

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

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getRuleTypeName() {
        return ruleTypeName;
    }

    public void setRuleTypeName(String ruleTypeName) {
        this.ruleTypeName = ruleTypeName;
    }

    public String getRuleGrammar() {
        return ruleGrammar;
    }

    public void setRuleGrammar(String ruleGrammar) {
        this.ruleGrammar = ruleGrammar;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String toString() {
        return this.dsName + " " + this.schemaName + " " + this.tableId + " " + this.tableName + " " +
                this.outputTopic + " " + this.groupId + " " + this.groupName + " " + this.status + " " +
                this.orderId + " " + this.ruleTypeName + " " + this.ruleGrammar + " " + this.version;
    }
}
