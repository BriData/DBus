package com.creditease.dbus.domain.model;

import java.util.Date;

public class SinkerTopologyTable {
    private Integer id;

    private Integer sinkerTopoId;

    private String sinkerName;

    private Integer dsId;

    private String dsName;

    private Integer schemaId;

    private String schemaName;

    private Integer tableId;

    private String tableName;

    private String description;

    private Date updateTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getSinkerTopoId() {
        return sinkerTopoId;
    }

    public void setSinkerTopoId(Integer sinkerTopoId) {
        this.sinkerTopoId = sinkerTopoId;
    }

    public String getSinkerName() {
        return sinkerName;
    }

    public void setSinkerName(String sinkerName) {
        this.sinkerName = sinkerName;
    }

    public Integer getDsId() {
        return dsId;
    }

    public void setDsId(Integer dsId) {
        this.dsId = dsId;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public Integer getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Integer schemaId) {
        this.schemaId = schemaId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}