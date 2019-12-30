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


package com.creditease.dbus.stream.db2.dispatcher;

import com.creditease.dbus.stream.common.tools.IGenericMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * Created by zhenlinzhong on 2018/4/27.
 */
public class Db2GenericMessage implements IGenericMessage {
    /**
     * Schema fields
     */
    public static final String NAMESAPCE = "table_name";
    public static final String SCHEMA_ID = "schema_id";
    public static final String PAYLOAD = "payload";
    public static final String OFFSET = "offset";

    private String nameSpace;
    private String schemaName;
    private String tableName;
    private int schemaId;
    private byte[] payload;

    private String offset;

    @Override
    public String getNameSpace() {
        return nameSpace;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;

        String[] arr = nameSpace.split("\\.");
        this.schemaName = arr[0];
        this.tableName = arr[1];
    }

    //Db2不需要
    @Override
    public int getSchemaHash() {
        return 0;
    }

    @Override
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public int getRowCount() {
        return 1;
    }

    @Override
    public boolean isDML() {
        return true;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
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

    public GenericRecord generateRecord(Schema genericSchema) {
        GenericRecord record = new GenericData.Record(genericSchema);

        record.put(NAMESAPCE, this.nameSpace);
        record.put(SCHEMA_ID, this.schemaId);
        record.put(PAYLOAD, ByteBuffer.wrap(this.payload));
        record.put(OFFSET, this.offset);

        return record;
    }
}
