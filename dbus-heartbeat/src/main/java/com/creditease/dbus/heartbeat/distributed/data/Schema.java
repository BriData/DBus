package com.creditease.dbus.heartbeat.distributed.data;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface Schema {

    enum Type {
        INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT;

        private String name;

        Type() {
            this.name = this.name().toLowerCase(Locale.ROOT);
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            switch (this) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    return true;
            }
            return false;
        }
    }

    Type type();

    boolean isOptional();

    Object defaultValue();

    String name();

    Integer version();

    String doc();

    Map<String, String> parameters();

    Schema keySchema();

    Schema valueSchema();

    List<Field> fields();

    Field field(String fieldName);

    Schema schema();

}
