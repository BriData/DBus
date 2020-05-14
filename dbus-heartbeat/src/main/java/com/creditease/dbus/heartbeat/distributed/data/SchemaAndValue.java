package com.creditease.dbus.heartbeat.distributed.data;

import java.util.Objects;

public class SchemaAndValue {

    private final Schema schema;
    private final Object value;

    public static final SchemaAndValue NULL = new SchemaAndValue(null, null);

    public SchemaAndValue(Schema schema, Object value) {
        this.value = value;
        this.schema = schema;
    }

    public Schema schema() {
        return schema;
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaAndValue that = (SchemaAndValue) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, value);
    }

    @Override
    public String toString() {
        return "SchemaAndValue{" +
                "schema=" + schema +
                ", value=" + value +
                '}';
    }

}
