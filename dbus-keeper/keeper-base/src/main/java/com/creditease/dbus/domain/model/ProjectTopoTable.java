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

public class ProjectTopoTable {
    private Integer id;

    private Integer projectId;

    private Integer tableId;

    private Integer topoId;

    private String status;

    private String outputTopic;

    private String outputType;

    private Integer sinkId;

    private Integer outputListType;

    private Integer metaVer;

    private Date updateTime;

    private Byte schemaChangeFlag;

    private String fullpullCol;

    private String fullpullSplitShardSize;

    private String fullpullSplitStyle;

    private String fullpullCondition;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getProjectId() {
        return projectId;
    }

    public void setProjectId(Integer projectId) {
        this.projectId = projectId;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public Integer getTopoId() {
        return topoId;
    }

    public void setTopoId(Integer topoId) {
        this.topoId = topoId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public Integer getSinkId() {
        return sinkId;
    }

    public void setSinkId(Integer sinkId) {
        this.sinkId = sinkId;
    }

    public Integer getOutputListType() {
        return outputListType;
    }

    public void setOutputListType(Integer outputListType) {
        this.outputListType = outputListType;
    }

    public Integer getMetaVer() {
        return metaVer;
    }

    public void setMetaVer(Integer metaVer) {
        this.metaVer = metaVer;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Byte getSchemaChangeFlag() {
        return schemaChangeFlag;
    }

    public void setSchemaChangeFlag(Byte schemaChangeFlag) {
        this.schemaChangeFlag = schemaChangeFlag;
    }

    public String getFullpullCol() {
        return fullpullCol;
    }

    public void setFullpullCol(String fullpullCol) {
        this.fullpullCol = fullpullCol;
    }

    public String getFullpullSplitShardSize() {
        return fullpullSplitShardSize;
    }

    public void setFullpullSplitShardSize(String fullpullSplitShardSize) {
        this.fullpullSplitShardSize = fullpullSplitShardSize;
    }

    public String getFullpullSplitStyle() {
        return fullpullSplitStyle;
    }

    public void setFullpullSplitStyle(String fullpullSplitStyle) {
        this.fullpullSplitStyle = fullpullSplitStyle;
    }

    public String getFullpullCondition() {
        return fullpullCondition;
    }

    public void setFullpullCondition(String fullpullCondition) {
        this.fullpullCondition = fullpullCondition;
    }
}
