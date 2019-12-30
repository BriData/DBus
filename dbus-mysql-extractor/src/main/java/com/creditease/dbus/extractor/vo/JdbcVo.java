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


package com.creditease.dbus.extractor.vo;

import java.io.Serializable;

public class JdbcVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -3206998078889776211L;

    private String type;

    private String key;

    private String driverClass;

    private String url;

    private String userName;

    private String password;

    private String ctrlTopic;

    private Integer initialSize = 1;

    private Integer maxActive = 1;

    private Integer maxIdle = 1;

    private Integer minIdle = 1;

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(Integer initialSize) {
        this.initialSize = initialSize;
    }

    public Integer getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(Integer maxActive) {
        this.maxActive = maxActive;
    }

    public Integer getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(Integer maxIdle) {
        this.maxIdle = maxIdle;
    }

    public Integer getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(Integer minIdle) {
        this.minIdle = minIdle;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getCtrlTopic() {
        return ctrlTopic;
    }

    public void setCtrlTopic(String ctrlTopic) {
        this.ctrlTopic = ctrlTopic;
    }

    @Override
    public String toString() {
        return "JdbcVo [type=" + type + ", key=" + key + ", driverClass=" + driverClass + ", url=" + url + ", userName="
                + userName + ", password=" + password + ", ctrlTopic=" + ctrlTopic + ", initialSize=" + initialSize
                + ", maxActive=" + maxActive + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + "]";
    }

}
