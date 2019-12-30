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

/**
 * User: 王少楠
 * Date: 2018-06-15
 * Time: 下午4:53
 * Desc: 加线时,getPath接口返回给前端的数据
 */
public class TopologyStartUpBean {
    private String dsName;

    private String topolotyType;

    private String topolotyName;

    private String status;

    private String jarPath;

    private String jarName;


    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getTopolotyType() {
        return topolotyType;
    }

    public void setTopolotyType(String topolotyType) {
        this.topolotyType = topolotyType;
    }

    public String getTopolotyName() {
        return topolotyName;
    }

    public void setTopolotyName(String topolotyName) {
        this.topolotyName = topolotyName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getJarName() {
        return jarName;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }
}
