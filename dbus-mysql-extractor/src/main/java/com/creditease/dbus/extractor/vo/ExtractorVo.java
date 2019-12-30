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


package com.creditease.dbus.extractor.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.StringUtils;

public class ExtractorVo {
    @JsonProperty("canal.instance.name")
    private String canalInstanceName;

    @JsonProperty("database.type")
    private String dbType;

    @JsonProperty("database.name")
    private String dbName;

    @JsonProperty("canal.client.batch.size")
    private Integer canalBatchSize;

    @JsonProperty("canal.client.flow.size")
    private Integer canalFlowSize = 50;

    @JsonProperty("canal.client.subscribe.filter")
    @JsonInclude(Include.NON_EMPTY)
    private String subscribeFilter;

    @JsonProperty("canal.zk.path")
    @JsonInclude(Include.NON_EMPTY)
    private String canalZkPath;

    @JsonProperty("kafka.send.batch.size")
    @JsonInclude(Include.NON_NULL)
    private Integer kafkaSendBatchSize;

    @JsonProperty("table.partition.regex")
    private String partitionTableRegex;

    public String getCanalInstanceName() {
        return canalInstanceName;
    }

    public void setCanalInstanceName(String canalInstanceName) {
        this.canalInstanceName = canalInstanceName;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Integer getCanalBatchSize() {
        return canalBatchSize;
    }

    public void setCanalBatchSize(Integer canalBatchSize) {
        this.canalBatchSize = canalBatchSize;
    }

    public String getSubscribeFilter() {
        if (StringUtils.isEmpty(subscribeFilter))
            return "";
        return subscribeFilter;
    }

    public void setSubscribeFilter(String subscribeFilter) {
        this.subscribeFilter = subscribeFilter;
    }

    public String getCanalZkPath() {
        return canalZkPath;
    }

    public void setCanalZkPath(String canalZkPath) {
        this.canalZkPath = canalZkPath;
    }

    public Integer getKafkaSendBatchSize() {
        return kafkaSendBatchSize;
    }

    public void setKafkaSendBatchSize(Integer kafkaSendBatchSize) {
        this.kafkaSendBatchSize = kafkaSendBatchSize;
    }

    public String getPartitionTableRegex() {
        return partitionTableRegex;
    }

    public void setPartitionTableRegex(String partitionTableRegex) {
        this.partitionTableRegex = partitionTableRegex;
    }

    @Override
    public String toString() {
        return "ExtractorVo [canalInstanceName=" + canalInstanceName + ", dbType=" + dbType + ", dbName=" + dbName
                + ", canalBatchSize=" + canalBatchSize + ", subscribeFilter=" + subscribeFilter
                + ", kafkaSendBatchSize=" + kafkaSendBatchSize + ", partitionTableRegex=" + partitionTableRegex + "]";
    }

    public Integer getCanalFlowSize() {
        return canalFlowSize;
    }

    public void setCanalFlowSize(Integer canalFlowSize) {
        this.canalFlowSize = canalFlowSize;
    }
}
