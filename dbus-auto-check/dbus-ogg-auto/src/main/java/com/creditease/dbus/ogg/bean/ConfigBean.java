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


package com.creditease.dbus.ogg.bean;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class ConfigBean {
    private String oggBigHome;

    private String dsName;

    private String oggUser;

    private String oggPwd;

    private String oggUrl;

    private String kafkaProducerName;

    private String kafkaUrl;

    private String[] tables;

    private String[] appendTables;

    private String nlsLang;

    public String getNlsLang() {
        return nlsLang;
    }

    public void setNlsLang(String nlsLang) {
        this.nlsLang = nlsLang;
    }

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public String[] getAppendTables() {
        return appendTables;
    }

    public void setAppendTables(String[] appendTables) {
        this.appendTables = appendTables;
    }

    public String getOggBigHome() {
        return oggBigHome;
    }

    public void setOggBigHome(String oggBigHome) {
        this.oggBigHome = oggBigHome;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getOggUser() {
        return oggUser;
    }

    public void setOggUser(String oggUser) {
        this.oggUser = oggUser;
    }

    public String getOggPwd() {
        return oggPwd;
    }

    public void setOggPwd(String oggPwd) {
        this.oggPwd = oggPwd;
    }

    public String getOggUrl() {
        return oggUrl;
    }

    public void setOggUrl(String oggUrl) {
        this.oggUrl = oggUrl;
    }

    public String getKafkaProducerName() {
        return kafkaProducerName;
    }

    public void setKafkaProducerName(String kafkaProducerName) {
        this.kafkaProducerName = kafkaProducerName;
    }

    public String getKafkaUrl() {
        return kafkaUrl;
    }

    public void setKafkaUrl(String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }
}
