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

package com.creditease.dbus.stream.common.appender.bean;

import java.io.Serializable;
import java.util.Date;

import static com.creditease.dbus.commons.Constants.DataTableStatus.*;

/**
 * Created by Shrimp on 16/6/15.
 */
public class DataTable implements Serializable {
    public static final String STATUS_OK = DATA_STATUS_OK;
    public static final String STATUS_ABORT = DATA_STATUS_ABORT;
    public static final String STATUS_WAITING = DATA_STATUS_WAITING;

    /** meta变更标志，初始值，未接收到meta变更事件*/
    public static final int META_FLAG_DEFAULT = 0;
    /** meta变更标志已经接收到meta变更事件，系统需要同步meta*/
    public static final int META_FLAG_CHANGED = 1;

    private Long id;
    private Long dsId;
    private Long schemaId;
    private String schema;
    private String tableName;
    private String physicalTableRegex;
    private String outputTopic;
    private Long verId;
    private Date createTime;
    private String status;
    private int metaChangeFlg;
    private int batchId;
    private int isOpen;
    private boolean isAutocomplete;
    /**
     * 是否在ums中输出before update，0表示不输出，1表示输出，在管理库中默认为0
     */
    private int outputBeforeUpdateFlg;

    public int getOutputBeforeUpdateFlg() {
        return outputBeforeUpdateFlg;
    }

    public void setOutputBeforeUpdateFlg(int outputBeforeUpdateFlg) {
        this.outputBeforeUpdateFlg = outputBeforeUpdateFlg;
    }

    public int getBatchId() {
        return batchId;
    }

    public void setBatchId(int batchId) {
        this.batchId = batchId;
    }

    public boolean isMetaChanged() {
        return META_FLAG_CHANGED == metaChangeFlg;
    }
    public int getMetaChangeFlg() {
        return metaChangeFlg;
    }

    public void setMetaChangeFlg(int metaChangeFlg) {
        this.metaChangeFlg = metaChangeFlg;
    }

    public boolean isPartationTable() {
        return !tableName.equals(physicalTableRegex);
    }
    public boolean isOk() {
        return STATUS_OK.equals(this.status);
    }

    public boolean isAbort() {
        return STATUS_ABORT.equals(this.status);
    }

    public void waiting() {
        setStatus(STATUS_WAITING);
    }
    public void ok() {
        setStatus(STATUS_OK);
    }
    public void abort() {
        setStatus(STATUS_ABORT);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDsId() {
        return dsId;
    }

    public void setDsId(Long dsId) {
        this.dsId = dsId;
    }

    public Long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Long schemaId) {
        this.schemaId = schemaId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPhysicalTableRegex() {
        return physicalTableRegex;
    }

    public void setPhysicalTableRegex(String physicalTableRegex) {
        this.physicalTableRegex = physicalTableRegex;
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

    public Long getVerId() {
        return verId;
    }

    public void setVerId(Long verId) {
        this.verId = verId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getIsOpen() {
        return isOpen;
    }

    public void setIsOpen(int isOpen) {
        this.isOpen = isOpen;
    }

    public boolean getIsAutocomplete() {
        return isAutocomplete;
    }

    public void setIsAutocomplete(boolean isAutocomplete) {
        this.isAutocomplete = isAutocomplete;
    }
}
