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


package com.creditease.dbus.stream.common.appender.bolt.processor.stat;

import java.util.Map;

/**
 * Created by Shrimp on 16/8/26.
 */
public class HeartbeatPulse {
    private long id;
    private String dsName;
    private String schemaName;
    private String tableName;
    private String packet;

    public static HeartbeatPulse build(Map<String, Object> data) {
        HeartbeatPulse pulse = new HeartbeatPulse();
        pulse.setId(Long.parseLong(data.get("ID").toString()));
        pulse.setDsName(data.get("DS_NAME").toString());
        pulse.setSchemaName(data.get("SCHEMA_NAME").toString());
        pulse.setTableName(data.get("TABLE_NAME").toString());
        pulse.setPacket(data.get("PACKET").toString());
        return pulse;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPacket() {
        return packet;
    }

    public void setPacket(String packet) {
        this.packet = packet;
    }
}
