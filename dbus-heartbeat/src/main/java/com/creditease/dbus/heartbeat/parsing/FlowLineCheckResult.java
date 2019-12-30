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

import org.apache.commons.lang.StringUtils;

/**
 * Created by mal on 2019/5/13.
 */
public class FlowLineCheckResult {

    private String dsType;

    private String dsName;

    private String canalPid;

    private String topoStatus;

    private String oggStatus;

    private String slaveTime;

    private String masterTime;

    private long diffTime;

    private String diffDate;

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getCanalPid() {
        return canalPid;
    }

    public void setCanalPid(String canalPid) {
        this.canalPid = canalPid;
    }

    public String getTopoStatus() {
        return topoStatus;
    }

    public void setTopoStatus(String topoStatus) {
        this.topoStatus = topoStatus;
    }

    public String getOggStatus() {
        return oggStatus;
    }

    public void setOggStatus(String oggStatus) {
        this.oggStatus = oggStatus;
    }

    public String getSlaveTime() {
        return slaveTime;
    }

    public void setSlaveTime(String slaveTime) {
        this.slaveTime = slaveTime;
    }

    public String getMasterTime() {
        return masterTime;
    }

    public void setMasterTime(String masterTime) {
        this.masterTime = masterTime;
    }

    public long getDiffTime() {
        return diffTime;
    }

    public void setDiffTime(long diffTime) {
        this.diffTime = diffTime;
    }

    public String getDiffDate() {
        return diffDate;
    }

    public void setDiffDate(String diffDate) {
        this.diffDate = diffDate;
    }

    public String toHtml() {
        StringBuilder html = new StringBuilder();
        html.append("<table bgcolor=\"#c1c1c1\">");
        html.append("    <tr bgcolor=\"#ffffff\">");
        if (StringUtils.equalsIgnoreCase("oracle", getDsType())) {
            html.append("        <th>Ogg Status</th>");
        } else if (StringUtils.equalsIgnoreCase("mysql", getDsType())) {
            html.append("        <th>Canal Status</th>");
        }
        html.append("        <th>Topo Status</th>");
        html.append("        <th>Master Time</th>");
        html.append("        <th>Slave Time</th>");
        html.append("        <th>Diff Date</th>");
        html.append("    </tr>");
        html.append("    <tr bgcolor=\"#ffffff\">");
        if (StringUtils.equalsIgnoreCase("oracle", getDsType())) {
            html.append("        <th align=\"left\">" + getOggStatus() + "</th>");
        } else if (StringUtils.equalsIgnoreCase("mysql", getDsType())) {
            html.append("        <th align=\"left\">" + getCanalPid() + "</th>");
        }
        html.append("        <th align=\"left\">" + getTopoStatus() + "</th>");
        html.append("        <th align=\"left\">" + getMasterTime() + "</th>");
        html.append("        <th align=\"left\">" + getSlaveTime() + "</th>");
        html.append("        <th align=\"left\">" + getDiffDate() + "</th>");
        html.append("    </tr>");
        html.append("</table>");
        return html.toString();
    }

}
