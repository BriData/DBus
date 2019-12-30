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


package com.creditease.dbus.heartbeat.parsing;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

/**
 * Created by mal on 2019/5/13.
 */
public class FlowLineCheckParser extends RespParser<FlowLineCheckResult> {

    public FlowLineCheckParser(String resp) {
        super(resp);
    }

    @Override
    public FlowLineCheckResult parse() {
        FlowLineCheckResult result = null;
        if (this.isSuccess()) {
            result = new FlowLineCheckResult();
            result.setDsType(this.getDsType());
            result.setDsName(this.getDsName());
            JSONObject masterSlaveJson = this.payload().getJSONObject("masterSlaveDiffDate");
            result.setMasterTime(this.getMasterLatestTime(masterSlaveJson));
            result.setSlaveTime(this.getSlaveLatestTime(masterSlaveJson));
            result.setDiffDate(this.getDiffDate(masterSlaveJson));
            result.setDiffTime(this.getDiffTime(masterSlaveJson));
            result.setTopoStatus(this.getTopologyStatus());
            result.setCanalPid(this.getCanalPid());
            result.setOggStatus(this.getOggStatus());
        }
        return result;
    }

    private String getDsType() {
        return this.payload().getString("dsType");
    }

    private String getDsName() {
        return this.payload().getString("dsName");
    }

    private String getMasterLatestTime(JSONObject masterSlaveJson) {
        return masterSlaveJson.getString("masterTime");
    }

    private String getSlaveLatestTime(JSONObject masterSlaveJson) {
        return masterSlaveJson.getString("slaveTime");
    }

    private String getDiffDate(JSONObject masterSlaveJson) {
        return masterSlaveJson.getString("diffDate");
    }

    private Long getDiffTime(JSONObject masterSlaveJson) {
        return masterSlaveJson.getLong("diffTime");
    }

    private String getCanalPid() {
        return StringUtils.defaultIfEmpty(this.payload().getString("canalPid"), StringUtils.EMPTY);
    }

    private String getOggStatus() {
        return StringUtils.defaultIfEmpty(this.payload().getString("oggStatus"), StringUtils.EMPTY);
    }

    private String getTopologyStatus() {
        JSONObject topoJson = this.payload().getJSONObject("topologyStatus");
        return (topoJson == null) ? StringUtils.EMPTY : topoJson.toJSONString().replaceAll("\\{|\\}|\"", StringUtils.EMPTY);
    }

}
