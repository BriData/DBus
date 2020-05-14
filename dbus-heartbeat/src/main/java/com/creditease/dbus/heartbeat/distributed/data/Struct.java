package com.creditease.dbus.heartbeat.distributed.data;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.creditease.dbus.heartbeat.distributed.errors.DataException;

public class Struct {

    private final Schema schema;
    private final Object[] values;

    public Struct(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT)
            throw new DataException("Not a struct schema: " + schema);
        this.schema = schema;
        this.values = new Object[schema.fields().size()];
    }

    public Schema schema() {
        return schema;
    }

    public Object get(String fieldName) {
        Field field = lookupField(fieldName);
        return get(field);
    }

    public Object get(Field field) {
        Object val = values[field.index()];
        if (val == null && field.schema().defaultValue() != null) {
            val = field.schema().defaultValue();
        }
        return val;
    }

    public Object getWithoutDefault(String fieldName) {
        Field field = lookupField(fieldName);
        return values[field.index()];
    }

    public Byte getInt8(String fieldName) {
        return (Byte) getCheckType(fieldName, Schema.Type.INT8);
    }

    public Short getInt16(String fieldName) {
        return (Short) getCheckType(fieldName, Schema.Type.INT16);
    }

    public Integer getInt32(String fieldName) {
        return (Integer) getCheckType(fieldName, Schema.Type.INT32);
    }

    public Long getInt64(String fieldName) {
        return (Long) getCheckType(fieldName, Schema.Type.INT64);
    }

    public Float getFloat32(String fieldName) {
        return (Float) getCheckType(fieldName, Schema.Type.FLOAT32);
    }

    public Double getFloat64(String fieldName) {
        return (Double) getCheckType(fieldName, Schema.Type.FLOAT64);
    }

    public Boolean getBoolean(String fieldName) {
        return (Boolean) getCheckType(fieldName, Schema.Type.BOOLEAN);
    }

    public String getString(String fieldName) {
        return (String) getCheckType(fieldName, Schema.Type.STRING);
    }

    public byte[] getBytes(String fieldName) {
        Object bytes = getCheckType(fieldName, Schema.Type.BYTES);
        if (bytes instanceof ByteBuffer)
            return ((ByteBuffer) bytes).array();
        return (byte[]) bytes;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getArray(String fieldName) {
        return (List<T>) getCheckType(fieldName, Schema.Type.ARRAY);
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String fieldName) {
        return (Map<K, V>) getCheckType(fieldName, Schema.Type.MAP);
    }

    public Struct getStruct(String fieldName) {
        return (Struct) getCheckType(fieldName, Schema.Type.STRUCT);
    }

    public Struct put(String fieldName, Object value) {
        Field field = lookupField(fieldName);
        return put(field, value);
    }

    public Struct put(Field field, Object value) {
        HeartbeatSchema.validateValue(field.schema(), value);
        values[field.index()] = value;
        return this;
    }

    public void validate() {
        for (Field field : schema.fields()) {
            Schema fieldSchema = field.schema();
            Object value = values[field.index()];
            if (value == null && (fieldSchema.isOptional() || fieldSchema.defaultValue() != null))
                continue;
            HeartbeatSchema.validateValue(fieldSchema, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Struct struct = (Struct) o;
        return Objects.equals(schema, struct.schema) &&
                Arrays.equals(values, struct.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, Arrays.hashCode(values));
    }

    private Field lookupField(String fieldName) {
        Field field = schema.field(fieldName);
        if (field == null)
            throw new DataException(fieldName + " is not a valid field name");
        return field;
    }

    // Get the field's value, but also check that the field matches the specified type, throwing an exception if it doesn't.
    // Used to implement the get*() methods that return typed data instead of Object
    private Object getCheckType(String fieldName, Schema.Type type) {
        Field field = lookupField(fieldName);
        if (field.schema().type() != type)
            throw new DataException("Field '" + fieldName + "' is not of type " + type);
        return values[field.index()];
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Struct{");
        boolean first = true;
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null) {
                final Field field = schema.fields().get(i);
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(field.name()).append("=").append(value);
            }
        }
        return sb.append("}").toString();
    }

}
