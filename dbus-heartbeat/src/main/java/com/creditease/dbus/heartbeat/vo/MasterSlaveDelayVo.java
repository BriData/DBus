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


package com.creditease.dbus.heartbeat.vo;

import java.io.Serializable;

/**
 * Created by mal on 2018/3/8.
 */
public class MasterSlaveDelayVo implements Serializable {

    private String masterLatestTime;

    private String slaveLatestTime;

    private Long diff;

    private String strDiff;

    private Long synTime;

    public String getMasterLatestTime() {
        return masterLatestTime;
    }

    public void setMasterLatestTime(String masterLatestTime) {
        this.masterLatestTime = masterLatestTime;
    }

    public String getSlaveLatestTime() {
        return slaveLatestTime;
    }

    public void setSlaveLatestTime(String slaveLatestTime) {
        this.slaveLatestTime = slaveLatestTime;
    }

    public Long getDiff() {
        return diff;
    }

    public void setDiff(Long diff) {
        this.diff = diff;
    }

    public String getStrDiff() {
        return strDiff;
    }

    public void setStrDiff(String strDiff) {
        this.strDiff = strDiff;
    }

    public Long getSynTime() {
        return synTime;
    }

    public void setSynTime(Long synTime) {
        this.synTime = synTime;
    }
}
