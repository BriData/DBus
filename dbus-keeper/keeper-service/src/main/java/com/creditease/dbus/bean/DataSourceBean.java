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


public class DataSourceBean {
    private Integer id;
    private String dsName;
    private String masterUrlOri;
    private String masterUrlIp;
    private String slaveUrlOri;
    private String slaveUrlIp;
    private String user;
    private String pass;
    private String schemas;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getMasterUrlOri() {
        return masterUrlOri;
    }

    public void setMasterUrlOri(String masterUrlOri) {
        this.masterUrlOri = masterUrlOri;
    }

    public String getMasterUrlIp() {
        return masterUrlIp;
    }

    public void setMasterUrlIp(String masterUrlIp) {
        this.masterUrlIp = masterUrlIp;
    }

    public String getSlaveUrlOri() {
        return slaveUrlOri;
    }

    public void setSlaveUrlOri(String slaveUrlOri) {
        this.slaveUrlOri = slaveUrlOri;
    }

    public String getSlaveUrlIp() {
        return slaveUrlIp;
    }

    public void setSlaveUrlIp(String slaveUrlIp) {
        this.slaveUrlIp = slaveUrlIp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
    }
}
