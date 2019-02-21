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

package com.creditease.dbus.router.bean;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by mal on 2018/5/29.
 */
public class EmitWarp<T> implements Serializable {

    private String key = null;

    private String nameSpace = null;

    private Long tableId = null;

    private Long hbTime = null;

    private T data = null;

    private String dsName = null;

    private String schemaName = null;

    private String tableName = null;

    private Integer size = null;

    public EmitWarp(String key) {
        this.key = key;
        if (isHB() || isUMS()) {
            String[] vals = StringUtils.split(key, ".");
            dsName = StringUtils.split(vals[2], "!")[0];
            schemaName = vals[3];
            tableName = vals[4];
            nameSpace = StringUtils.joinWith(".", dsName, schemaName, tableName);
            if (isHB()) {
                if (StringUtils.contains(vals[8], "|")) {
                    String times[] = StringUtils.split(vals[8], "|");
                    hbTime = Long.valueOf(times[0]);
                }
            }
        }
    }

    public boolean isHB() {
        String[] vals = StringUtils.split(key, ".");
        return StringUtils.equals(vals[0], "data_increment_heartbeat");
    }

    public boolean isUMS() {
        String[] vals = StringUtils.split(key, ".");
        return StringUtils.equals(vals[0], "data_increment_data");
    }

    public boolean isCtrl() {
        return StringUtils.equals(key, "ctrl");
    }

    public boolean isStat() {
        return StringUtils.equals(key, "stat");
    }

    public boolean isMysql() {
        boolean ret = false;
        String[] vals = StringUtils.split(key, ".");
        if (vals != null && vals.length > 2) {
            ret = StringUtils.equalsIgnoreCase(vals[1], "mysql");
        }
        return ret;
    }

    public boolean isOracle() {
        boolean ret = false;
        String[] vals = StringUtils.split(key, ".");
        if (vals != null && vals.length > 2) {
            ret = StringUtils.equalsIgnoreCase(vals[1], "oracle");
        }
        return ret;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getDsName() {
        return dsName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Long getHbTime() {
        return hbTime;
    }

    public void setHbTime(Long hbTime) {
        this.hbTime = hbTime;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }
}
