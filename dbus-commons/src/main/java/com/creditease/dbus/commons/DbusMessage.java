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


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;

import java.util.*;

/**
 * 定义自解释的消息协议
 * Created by Shrimp on 16/5/20.
 */
public abstract class DbusMessage {
    protected Protocol protocol;
    protected Schema schema;
    protected List<Payload> payload;

    public DbusMessage() {
    }

    public DbusMessage(String version, ProtocolType type) {
        this.protocol = new Protocol(type, version);
        this.payload = new ArrayList<>();
    }

    public Object messageValue(String fieldName, int row) {
        int idx = schema.index(fieldName);
        if (idx < 0) return null;
        return payload.get(row).getTuple().get(idx);
    }

    public void setMessageValue(String fieldName, Object value, int row) {
        int idx = schema.index(fieldName);
        if (idx >= 0) {
            payload.get(row).getTuple().set(idx, value);
        }
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public Schema getSchema() {
        return schema;
    }

    public List<Payload> getPayload() {
        return payload;
    }

    public int payloadSizeWithoutBefore() {
        int operationIndex = schema.index(Field._UMS_OP_);
        int result = 0;
        for (Payload onePayload : payload) {
            Object operation = onePayload.getTuple().get(operationIndex);
            if ("b".equals(operation)) continue;
            result++;
        }
        return result;
    }

    public void addTuple(Object[] tuple) {
        addTuple(this.payload.size(), Arrays.asList(tuple));
    }

    public void addTuple(int idx, List<Object> list) {
        if (this.payload.size() <= idx) {
            this.payload.add(new Payload());
        }
        this.payload.get(idx).getTuple().addAll(list);
    }

    public boolean containsFiled(String fieldName) {
        return schema.index(fieldName) >= 0;
    }

    public static class Protocol {
        private ProtocolType type;
        private String version;

        public Protocol(ProtocolType type, String version) {
            this.type = type;
            this.version = version;
        }

        public String getType() {
            return type.toString();
        }

        public String getVersion() {
            return version;
        }

    }

    public static abstract class Schema {
        private String namespace;
        private Map<String, Integer> index;
        private List<Field> fields;

        public Schema() {
        }

        public void addField(String name, DataType type, boolean nullable) {
            index.put(name, this.fields.size());
            this.fields.add(new Field(name, type, nullable));
        }

        public void addField(String name, DataType type, boolean nullable, boolean encoded) {
            index.put(name, this.fields.size());
            Field field = new Field(name, type, nullable);
            field.setEncoded(encoded);
            this.fields.add(field);
        }

        public Schema(String schemaNs) {
            this.namespace = schemaNs;
            this.fields = new ArrayList<>();
            index = new HashMap<>();
        }

        public String getNamespace() {
            return namespace;
        }

        public List<Field> getFields() {
            return fields;
        }

        public Integer index(String name) {
            return index.containsKey(name) ? index.get(name) : -1;
        }

        //for DbusMessage14 addUnField
        public void setIndex(String name, Integer size) {
            index.put(name, size);
        }

        public Field field(int idx) {
            return this.fields.get(idx);
        }

        public Field field(String name) {
            return field(index(name));
        }
    }

    public static class Field {
        public static final String _UMS_UID_ = "ums_uid_";
        public static final String _UMS_ID_ = "ums_id_";
        public static final String _UMS_TS_ = "ums_ts_";
        public static final String _UMS_OP_ = "ums_op_";
        //public static final String HEARTBEAT_TS = "heartbeat_ts";
        //public static final String TERMINATION_TS = "termination_ts";

        private String name;
        private DataType type;
        private boolean nullable;
        private boolean encoded;

        public Field() {
        }

        public Field(String name, DataType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        public boolean isEncoded() {
            return encoded;
        }

        public void setEncoded(boolean encoded) {
            this.encoded = encoded;
        }

        public String getName() {
            return name;
        }

        public DataType dataType() {
            return type;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getType() {
            return type.toString();
        }
    }


    public static class Payload {
        private List<Object> tuple;

        public Payload() {
            this.tuple = new LinkedList<>();
        }

        public List<Object> getTuple() {
            return tuple;
        }
    }

    public enum ProtocolType {
        DATA_INITIAL_DATA,
        DATA_INCREMENT_DATA,
        DATA_INCREMENT_TERMINATION,
        DATA_INCREMENT_HEARTBEAT;

        private String value;

        ProtocolType() {
            this.value = this.name().toLowerCase();
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
