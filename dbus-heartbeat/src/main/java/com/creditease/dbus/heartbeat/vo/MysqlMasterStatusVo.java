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


package com.creditease.dbus.heartbeat.vo;

public class MysqlMasterStatusVo {

    private String file;

    private long position;

    private String binlogDoDB;

    private String binlogIgnoreDB;

    private String executedGtidSet;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public String getBinlogDoDB() {
        return binlogDoDB;
    }

    public void setBinlogDoDB(String binlogDoDB) {
        this.binlogDoDB = binlogDoDB;
    }

    public String getBinlogIgnoreDB() {
        return binlogIgnoreDB;
    }

    public void setBinlogIgnoreDB(String binlogIgnoreDB) {
        this.binlogIgnoreDB = binlogIgnoreDB;
    }

    public String getExecutedGtidSet() {
        return executedGtidSet;
    }

    public void setExecutedGtidSet(String executedGtidSet) {
        this.executedGtidSet = executedGtidSet;
    }
}
