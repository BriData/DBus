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


package com.creditease.dbus.stream.common.appender.bean;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by Shrimp on 16/5/18.
 */
public class AvroSchema implements Serializable {
    private long id;
    private long dsId;
    private String namespace;
    private String schemaName;
    private String fullName;
    private int schemaHash;
    private String schema;
    private Timestamp ts;

    public long getDsId() {
        return dsId;
    }

    public void setDsId(long dsId) {
        this.dsId = dsId;
    }

    public AvroSchema() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Timestamp getTs() {
        return ts;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
