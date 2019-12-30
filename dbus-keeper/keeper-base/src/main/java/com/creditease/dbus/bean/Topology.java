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


package com.creditease.dbus.bean;

public class Topology {
    private String id;
    private String name;
    private String uptime;
    private String status;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    //"assignedTotalMem": 832,
    //"owner": "app",
    //"requestedMemOnHeap": 0,
    //"encodedId": "orcl-dispatcher-appender-18-1571993144",
    //"assignedMemOnHeap": 832,
    //"id": "orcl-dispatcher-appender-18-1571993144",
    //"uptime": "3d 22h 57m 48s",
    //"schedulerInfo": null,
    //"name": "orcl-dispatcher-appender",
    //"workersTotal": 1,
    //"status": "ACTIVE",
    //"requestedMemOffHeap": 0,
    //"tasksTotal": 15,
    //"requestedCpu": 0,
    //"replicationCount": 2,
    //"executorsTotal": 15,
    //"uptimeSeconds": 341868,
    //"assignedCpu": 0,
    //"assignedMemOffHeap": 0,
    //"requestedTotalMem": 0
}
