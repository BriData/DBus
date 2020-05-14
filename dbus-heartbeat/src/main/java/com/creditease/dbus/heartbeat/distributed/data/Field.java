package com.creditease.dbus.heartbeat.distributed.data;

import java.util.Objects;

public class Field {

    private final String name;
    private final int index;
    private final Schema schema;

    public Field(String name, int index, Schema schema) {
        this.name = name;
        this.index = index;
        this.schema = schema;
    }

    public String name() {
        return name;
    }

    public int index() {
        return index;
    }

    public Schema schema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return Objects.equals(index, field.index) &&
                Objects.equals(name, field.name) &&
                Objects.equals(schema, field.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index, schema);
    }

}
