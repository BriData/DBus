package com.creditease.dbus.stream.common.appender.bean;

import java.io.Serializable;

/**
 * Created by haowei6 on 2017/9/14.
 */
public class TableComments implements Serializable {
    private String owner;
    private String tableName;
    private String comments;

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
}
