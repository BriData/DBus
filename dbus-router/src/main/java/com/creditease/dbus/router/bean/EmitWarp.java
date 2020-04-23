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


package com.creditease.dbus.router.bean;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by mal on 2018/5/29.
 */
public class EmitWarp<T> implements Serializable {

    private String key = null;

    private String nameSpace = null;

    private String aliasNameSpace = null;

    private Long tableId = null;

    private Long hbTime = null;

    private T data = null;

    private String dsName = null;

    private String schemaName = null;

    private String tableName = null;

    private Integer size = null;

    private Long offset = null;

    private String dsNameAlias = null;

    private boolean isUseAlias = true;

    public EmitWarp(String key) {
        this.key = key;
        if (isHB() || isUMS()) {
            String[] vals = StringUtils.split(key, ".");
            dsNameAlias = StringUtils.split(vals[2], "!")[0];
            schemaName = vals[3];
            tableName = vals[4];
            // 数据源的默认值和别名相同，是为了对老版本增量数据的兼容
            dsName = dsNameAlias;
            //--心跳
            //data_increment_heartbeat.mysql.mysql_db5.ip_settle.fin_credit_repayment_bill.0.0.0.1578894885250|1578894877783|ok|mysql_db5.wh_placeholder
            //--数据
            //data_increment_data.oracle.acc3.ACC.REQUEST_BUS_SHARDING_2018_0000.0.0.0.1577810117698|acc3.wh_placeholder
            // 完成数据源别名到真实名称的转换
            if (isHB()) {
                if (StringUtils.contains(vals[8], "|")) {
                    String times[] = StringUtils.split(vals[8], "|");
                    hbTime = Long.valueOf(times[0]);
                    if (times.length == 4) {
                        dsName = times[3];
                        isUseAlias =  false;
                    }
                }
            } else if (isUMS()) {
                if (StringUtils.contains(vals[8], "|")) {
                    String times[] = StringUtils.split(vals[8], "|");
                    if (times.length == 2) {
                        dsName = times[1];
                        isUseAlias =  false;
                    }
                }
            }
            nameSpace = StringUtils.joinWith(".", dsName, schemaName, tableName);
            aliasNameSpace = StringUtils.joinWith(".", dsNameAlias, schemaName, tableName);
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

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getDsNameAlias() {
        return dsNameAlias;
    }

    public boolean isUseAlias() {
        return isUseAlias;
    }

    public String getAliasNameSpace() {
        return aliasNameSpace;
    }
}
