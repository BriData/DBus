package com.creditease.dbus.heartbeat.distributed.data.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.creditease.dbus.heartbeat.distributed.data.Convert;
import com.creditease.dbus.heartbeat.distributed.data.Field;
import com.creditease.dbus.heartbeat.distributed.data.Schema;
import com.creditease.dbus.heartbeat.distributed.data.SchemaAndValue;
import com.creditease.dbus.heartbeat.distributed.data.Struct;
import com.creditease.dbus.heartbeat.distributed.errors.DataException;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonConvert implements Convert {

    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    private static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;

    private static final HashMap<Schema.Type, JsonToTypeConverter> TO_CONNECT_CONVERTERS = new HashMap<>();

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.booleanValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return (byte) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return (short) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.longValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.floatValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.doubleValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                try {
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Invalid bytes field", e);
                }
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.textValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                Schema elemSchema = schema == null ? null : schema.valueSchema();
                ArrayList<Object> result = new ArrayList<>();
                for (JsonNode elem : value) {
                    result.add(convertToConnect(elemSchema, elem));
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();

                Map<Object, Object> result = new HashMap<>();
                if (schema == null || keySchema.type() == Schema.Type.STRING) {
                    if (!value.isObject())
                        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fieldIt.next();
                        result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
                    }
                } else {
                    if (!value.isArray())
                        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    for (JsonNode entry : value) {
                        if (!entry.isArray())
                            throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                        if (entry.size() != 2)
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                        result.put(convertToConnect(keySchema, entry.get(0)),
                                convertToConnect(valueSchema, entry.get(1)));
                    }
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (!value.isObject())
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields())
                    result.put(field, convertToConnect(field.schema(), value.get(field.name())));

                return result;
            }
        });
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] fromtData(String topic, Schema schema, Object value) {
        return new byte[0];
    }

    @Override
    public SchemaAndValue toData(String topic, byte[] value) {
        return null;
    }

    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue.isNull()) {
                if (schema.defaultValue() != null)
                    return schema.defaultValue();
                if (schema.isOptional())
                    return null;
                throw new DataException("Invalid null value for required " + schemaType +  " field");
            }
        } else {
            switch (jsonValue.getNodeType()) {
                case NULL:
                    return null;
                case BOOLEAN:
                    schemaType = Schema.Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber())
                        schemaType = Schema.Type.INT64;
                    else
                        schemaType = Schema.Type.FLOAT64;
                    break;
                case ARRAY:
                    schemaType = Schema.Type.ARRAY;
                    break;
                case OBJECT:
                    schemaType = Schema.Type.MAP;
                    break;
                case STRING:
                    schemaType = Schema.Type.STRING;
                    break;

                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
                    break;
            }
        }

        final JsonToTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + String.valueOf(schemaType));

        Object converted = typeConverter.convert(schema, jsonValue);

        return converted;
    }

    private interface JsonToTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

}
