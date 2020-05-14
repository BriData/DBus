package com.creditease.dbus.heartbeat.distributed.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.creditease.dbus.heartbeat.distributed.errors.DataException;
import com.creditease.dbus.heartbeat.distributed.errors.SchemaBuilderException;

public class SchemaBuilder implements Schema {

    private static final String TYPE_FIELD = "type";
    private static final String OPTIONAL_FIELD = "optional";
    private static final String DEFAULT_FIELD = "default";
    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";
    private static final String DOC_FIELD = "doc";

    private final Type type;
    private Boolean optional = null;
    private Object defaultValue = null;

    private List<Field> fields = null;
    private Schema keySchema = null;
    private Schema valueSchema = null;

    private String name;
    private Integer version;
    // Optional human readable documentation describing this schema.
    private String doc;
    // Additional parameters for logical types.
    private Map<String, String> parameters;

    private SchemaBuilder(Type type) {
        this.type = type;
        if (type == Type.STRUCT) {
            fields = new ArrayList<>();
        }
    }

    // Common/metadata fields

    @Override
    public boolean isOptional() {
        return optional == null ? false : optional;
    }

    public SchemaBuilder optional() {
        checkNull(OPTIONAL_FIELD, optional);
        optional = true;
        return this;
    }

    public SchemaBuilder required() {
        checkNull(OPTIONAL_FIELD, optional);
        optional = false;
        return this;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    public SchemaBuilder defaultValue(Object value) {
        checkNull(DEFAULT_FIELD, defaultValue);
        checkNotNull(TYPE_FIELD, type, DEFAULT_FIELD);
        try {
            HeartbeatSchema.validateValue(this, value);
        } catch (DataException e) {
            throw new SchemaBuilderException("Invalid default value", e);
        }
        defaultValue = value;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    public SchemaBuilder name(String name) {
        checkNull(NAME_FIELD, this.name);
        this.name = name;
        return this;
    }

    @Override
    public Integer version() {
        return version;
    }

    public SchemaBuilder version(Integer version) {
        checkNull(VERSION_FIELD, this.version);
        this.version = version;
        return this;
    }

    @Override
    public String doc() {
        return doc;
    }

    public SchemaBuilder doc(String doc) {
        checkNull(DOC_FIELD, this.doc);
        this.doc = doc;
        return this;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters == null ? null : Collections.unmodifiableMap(parameters);
    }

    public SchemaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.put(propertyName, propertyValue);
        return this;
    }

    public SchemaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty())
            return this;
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.putAll(props);
        return this;
    }

    @Override
    public Type type() {
        return type;
    }

    public static SchemaBuilder type(Type type) {
        return new SchemaBuilder(type);
    }

    public static SchemaBuilder int8() {
        return new SchemaBuilder(Type.INT8);
    }

    public static SchemaBuilder int16() {
        return new SchemaBuilder(Type.INT16);
    }

    public static SchemaBuilder int32() {
        return new SchemaBuilder(Type.INT32);
    }

    public static SchemaBuilder int64() {
        return new SchemaBuilder(Type.INT64);
    }

    public static SchemaBuilder float32() {
        return new SchemaBuilder(Type.FLOAT32);
    }

    public static SchemaBuilder float64() {
        return new SchemaBuilder(Type.FLOAT64);
    }

    public static SchemaBuilder bool() {
        return new SchemaBuilder(Type.BOOLEAN);
    }

    public static SchemaBuilder string() {
        return new SchemaBuilder(Type.STRING);
    }

    public static SchemaBuilder bytes() {
        return new SchemaBuilder(Type.BYTES);
    }

    public static SchemaBuilder struct() {
        return new SchemaBuilder(Type.STRUCT);
    }

    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
        if (type != Type.STRUCT)
            throw new SchemaBuilderException("Cannot create fields on type " + type);
        int fieldIndex = fields.size();
        fields.add(new Field(fieldName, fieldIndex, fieldSchema));
        return this;
    }

    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        for (Field field : fields)
            if (field.name().equals(fieldName))
                return field;
        return null;
    }

    public static SchemaBuilder array(Schema valueSchema) {
        SchemaBuilder builder = new SchemaBuilder(Type.ARRAY);
        builder.valueSchema = valueSchema;
        return builder;
    }

    public static SchemaBuilder map(Schema keySchema, Schema valueSchema) {
        SchemaBuilder builder = new SchemaBuilder(Type.MAP);
        builder.keySchema = keySchema;
        builder.valueSchema = valueSchema;
        return builder;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }

    public Schema build() {
        return new HeartbeatSchema(type, isOptional(), defaultValue, name, version, doc,
                parameters == null ? null : Collections.unmodifiableMap(parameters),
                fields == null ? null : Collections.unmodifiableList(fields), keySchema, valueSchema);
    }

    @Override
    public Schema schema() {
        return build();
    }


    private static void checkNull(String fieldName, Object val) {
        if (val != null)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " has already been set.");
    }

    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
    }

}
