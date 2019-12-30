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


package com.creditease.dbus.stream.db2.appender.avro;

import java.io.Serializable;

/**
 * Avro Formatter generic wrapper schema
 * <p/>
 * {
 * "type": "record",
 * "name": "generic_wrapper",
 * "namespace": "oracle.goldengate",
 * "fields": [
 * {
 * "name": "table_name",
 * "type": "string"
 * },
 * {
 * "name": "schema_hash",
 * "type": "int"
 * },
 * {
 * "name": "payload",
 * "type": "bytes"
 * }
 * ]
 * }
 * <p/>
 * Created by Shrimp on 16/5/5.
 */
public class GenericData implements Serializable {
    /**
     * Schema fields
     */
    public static final String TABLE_NAME = "table_name";
    public static final String SCHEMA_HASH = "schema_hash";
    public static final String SCHEMA_ID = "schema_id";
    public static final String PAYLOAD = "payload";
    public static final String OFFSET = "offset";

    private String tableName;
    private int schemaHash;
    private int schemaId;
    private byte[] payload;
    private String offset;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(int schemaId) {
        this.schemaId = schemaId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }
}
