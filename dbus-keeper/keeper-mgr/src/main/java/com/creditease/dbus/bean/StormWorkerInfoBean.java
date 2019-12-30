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

public class StormWorkerInfoBean implements Comparable {

    private String host;
    private String topologyId;
    private String portFromUi;
    private String portFromShell;
    private String pidFromShell;
    private boolean isDeleted;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public String getPortFromUi() {
        return portFromUi;
    }

    public void setPortFromUi(String portFromUi) {
        this.portFromUi = portFromUi;
    }

    public String getPortFromShell() {
        return portFromShell;
    }

    public void setPortFromShell(String portFromShell) {
        this.portFromShell = portFromShell;
    }

    public String getPidFromShell() {
        return pidFromShell;
    }

    public void setPidFromShell(String pidFromShell) {
        this.pidFromShell = pidFromShell;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }


    @Override
    public int compareTo(Object object) {
        StormWorkerInfoBean bean = (StormWorkerInfoBean) object;
        if (this.host.compareTo(bean.host) > 0) {
            return 1;
        }
        if (this.host.compareTo(bean.host) < 0) {
            return -1;
        }
        if (this.topologyId.compareTo(bean.topologyId) > 0) {
            return 1;
        }
        if (this.topologyId.compareTo(bean.topologyId) < 0) {
            return -1;
        }
        return 0;
    }
}
