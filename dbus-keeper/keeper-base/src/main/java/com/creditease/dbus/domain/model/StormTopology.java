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

/**
 * Created by zhangyf on 16/10/26.
 */
public class StormTopology {

    private String topologyId;

    private String topologyName;

    private String uptime = "Not Online.";

    private String runningInfo = "Not Available.";

    //    private String topologyId;
    //    private String hostAndPort;
    public StormTopology(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getRunningInfo() {
        return runningInfo;
    }

    public void setRunningInfo(String runningInfo) {
        this.runningInfo = runningInfo;
    }
}
