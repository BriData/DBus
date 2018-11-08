/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.stream.common;

/**
 * Created by dongwang47 on 2016/8/17.
 */
public class DataSourceInfo {
    public String getDbSourceName() {
        return dbSourceName;
    }

    public void setDbSourceName(String dbSourceName) {
        this.dbSourceName = dbSourceName;
    }

    public String getDbSourceType() {
        return dbSourceType;
    }

    public void setDbSourceType(String dbSourceType) {
        this.dbSourceType = dbSourceType;
    }

    public String getDataTopic() {
        return dataTopic;
    }

    public void setDataTopic(String dataTopic) {
        this.dataTopic = dataTopic;
    }

    public String getCtrlTopic() {
        return ctrlTopic;
    }

    public void setCtrlTopic(String ctrlTopic) {
        this.ctrlTopic = ctrlTopic;
    }

    public String getDbusSchema() {
        return dbusSchema;
    }

    public void setDbusSchema(String dbusSchema) {
        this.dbusSchema = dbusSchema;
    }



    public String getDataTopicOffset() {
        return dataTopicOffset;
    }

    public void setDataTopicOffset(String dataTopicOffset) {
        this.dataTopicOffset = dataTopicOffset;
    }

    public void resetDataTopicOffset() {
        this.dataTopicOffset = "none";
    }

    public String getSchemaRegistryRestUrl() {
        return schemaRegistryRestUrl;
    }

    public void setSchemaRegistryRestUrl(String schemaRegistryRestUrl) {
        this.schemaRegistryRestUrl = schemaRegistryRestUrl;
    }

    private String dbSourceName = null;
    private String dbSourceType = null;
    private String dataTopic = null;
    private String ctrlTopic = null;
    private String dbusSchema = null;
    private String dataTopicOffset = null;
    private String schemaRegistryRestUrl = null;

    @Override
    public String toString() {
        return String.format("dsName=%s, dsType=%s, dataTopic=%s, ctrlTopic=%s, dbusSchema=%s, dataTopicOffset=%s",
                              dbSourceName, dbSourceType, dataTopic, ctrlTopic, dbusSchema, dataTopicOffset);
    }
}
