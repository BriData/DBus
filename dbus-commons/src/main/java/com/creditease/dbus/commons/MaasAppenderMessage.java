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


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;

/**
 * Created by haowei6 on 2017/9/19.
 */
public class MaasAppenderMessage {

    private long dsId;
    private String dsType;
    private String schemaName;
    private String tableName;
    private String tableComment;
    private String columnName;
    private String columnType;
    private String columnNullable;
    private String columnComment;
    private String type;
    private String ddl;

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnNullable() {
        return columnNullable;
    }

    public void setColumnNullable(String columnNullable) {
        this.columnNullable = columnNullable;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public long getDsId() {
        return dsId;
    }

    public void setDsId(long dsId) {
        this.dsId = dsId;
    }

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public static class Type {
        public static String ALTER = "ALTER";
        public static String COMMENT_TABLE = "COMMENT-TABLE";
        public static String COMMENT_COLUMN = "COMMENT-COLUMN";
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public static void main(String[] args) {
        MaasAppenderMessage mass = new MaasAppenderMessage();
        String s = JSON.toJSONString(mass);
        System.out.println(s);
        System.out.println(JSON.parseObject(s, MaasAppenderMessage.class).getColumnName());
    }
}
