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

public class DeployInfoBean {
    private String host;
    private String canalPath;
    private int maxCanalNumber;
    private int deployCanalNumber;
    private String oggTrailPath;
    private String oggToolPath;
    private String oggPath;
    private Integer mgrReplicatPort;
    private int maxReplicatNumber;
    private int deployReplicatNumber;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getCanalPath() {
        return canalPath;
    }

    public void setCanalPath(String canalPath) {
        this.canalPath = canalPath;
    }

    public int getMaxCanalNumber() {
        return maxCanalNumber;
    }

    public void setMaxCanalNumber(int maxCanalNumber) {
        this.maxCanalNumber = maxCanalNumber;
    }

    public int getDeployCanalNumber() {
        return deployCanalNumber;
    }

    public void setDeployCanalNumber(int deployCanalNumber) {
        this.deployCanalNumber = deployCanalNumber;
    }

    public String getOggTrailPath() {
        return oggTrailPath;
    }

    public void setOggTrailPath(String oggTrailPath) {
        this.oggTrailPath = oggTrailPath;
    }

    public String getOggToolPath() {
        return oggToolPath;
    }

    public void setOggToolPath(String oggToolPath) {
        this.oggToolPath = oggToolPath;
    }

    public String getOggPath() {
        return oggPath;
    }

    public void setOggPath(String oggPath) {
        this.oggPath = oggPath;
    }

    public Integer getMgrReplicatPort() {
        return mgrReplicatPort;
    }

    public void setMgrReplicatPort(Integer mgrReplicatPort) {
        this.mgrReplicatPort = mgrReplicatPort;
    }

    public int getMaxReplicatNumber() {
        return maxReplicatNumber;
    }

    public void setMaxReplicatNumber(int maxReplicatNumber) {
        this.maxReplicatNumber = maxReplicatNumber;
    }

    public int getDeployReplicatNumber() {
        return deployReplicatNumber;
    }

    public void setDeployReplicatNumber(int deployReplicatNumber) {
        this.deployReplicatNumber = deployReplicatNumber;
    }
}
