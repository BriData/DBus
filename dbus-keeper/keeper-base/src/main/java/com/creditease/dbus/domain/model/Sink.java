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

public class Sink {
    private Integer id;

    private String sinkName;

    private String sinkDesc;

    private String sinkType;

    private String url;

    private Date updateTime;

    private Byte isGlobal;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getSinkName() {
        return sinkName;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }

    public String getSinkDesc() {
        return sinkDesc;
    }

    public void setSinkDesc(String sinkDesc) {
        this.sinkDesc = sinkDesc;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Byte getIsGlobal() {
        return isGlobal;
    }

    public void setIsGlobal(Byte isGlobal) {
        this.isGlobal = isGlobal;
    }
}
