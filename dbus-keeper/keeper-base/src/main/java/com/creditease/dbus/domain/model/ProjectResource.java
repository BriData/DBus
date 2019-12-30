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


package com.creditease.dbus.domain.model;

import java.util.Date;

public class ProjectResource {

    /**
     * 不能拉全量
     */
    public static final byte FULL_PULL_ENABLE_FALSE = 0;
    /**
     * 可以拉全量
     */
    public static final byte FULL_PULL_ENABLE_TRUE = 1;

    /**
     * 状态
     */
    public static final String STATUS_USE = "use";
    public static final String STATUS_UNUSE = "unuse";

    private Integer id;

    private Integer projectId;

    private Integer tableId;

    private Byte fullpullEnableFlag;

    private Date updateTime;

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

    public Byte getFullpullEnableFlag() {
        return fullpullEnableFlag;
    }

    public void setFullpullEnableFlag(Byte fullpullEnableFlag) {
        this.fullpullEnableFlag = fullpullEnableFlag;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
