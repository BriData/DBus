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


package com.creditease.dbus.canal.bean;

/**
 * User: 王少楠
 * Date: 2018-08-02
 * Desc:
 */
public class DeployPropsBean {
    /**
     * 数据源名称
     */
    private String dsName;
    /**
     * zk地址
     */
    private String zkPath;
    /**
     * 备库地址
     */
    private String slavePath;
    /**
     * canal用户名
     */
    private String canalUser;
    /**
     * canal 用户密码
     */
    private String canalPwd;
    /**
     * canal slave id
     */
    private String canalSlaveId;
    /**
     *
     */
    private String bootstrapServers;

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getZkPath() {
        return zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public String getSlavePath() {
        return slavePath;
    }

    public void setSlavePath(String slavePath) {
        this.slavePath = slavePath;
    }

    public String getCanalUser() {
        return canalUser;
    }

    public void setCanalUser(String canalUser) {
        this.canalUser = canalUser;
    }

    public String getCanalPwd() {
        return canalPwd;
    }

    public void setCanalPwd(String canalPwd) {
        this.canalPwd = canalPwd;
    }

    public String getCanalSlaveId() {
        return canalSlaveId;
    }

    public void setCanalSlaveId(String canalSlaveId) {
        this.canalSlaveId = canalSlaveId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
