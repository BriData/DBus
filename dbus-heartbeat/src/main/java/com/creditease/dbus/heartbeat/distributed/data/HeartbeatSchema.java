package com.creditease.dbus.heartbeat.distributed.data;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.creditease.dbus.heartbeat.distributed.errors.DataException;

public class HeartbeatSchema implements Schema {

    private static final Map<Type, List<Class>> SCHEMA_TYPE_CLASSES = new HashMap<>();
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    private static final Map<Class<?>, Type> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        SCHEMA_TYPE_CLASSES.put(Type.INT8, Arrays.asList((Class) Byte.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT16, Arrays.asList((Class) Short.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Arrays.asList((Class) Integer.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Arrays.asList((Class) Long.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Arrays.asList((Class) Float.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Arrays.asList((Class) Double.class));
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Arrays.asList((Class) Boolean.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRING, Arrays.asList((Class) String.class));

        SCHEMA_TYPE_CLASSES.put(Type.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class));
        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, Arrays.asList((Class) List.class));
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Arrays.asList((Class) Map.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Arrays.asList((Class) Struct.class));

        for (Map.Entry<Type, List<Class>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
            for (Class<?> schemaClass : schemaClasses.getValue())
                JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
        }

        /*LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Arrays.asList((Class) BigDecimal.class));
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));*/
    }

    // The type of the field
    private final Type type;
    private final boolean optional;
    private final Object defaultValue;

    private final List<Field> fields;
    private final Map<String, Field> fieldsByName;

    private final Schema keySchema;
    private final Schema valueSchema;

    private final String name;
    private final Integer version;
    private final String doc;
    private final Map<String, String> parameters;

    public HeartbeatSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc, Map<String, String> parameters, List<Field> fields, Schema keySchema, Schema valueSchema) {
        this.type = type;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        this.parameters = parameters;

        if (this.type == Type.STRUCT) {
            this.fields = fields == null ? Collections.<Field>emptyList() : fields;
            this.fieldsByName = new HashMap<>(this.fields.size());
            for (Field field : this.fields)
                fieldsByName.put(field.name(), field);
        } else {
            this.fields = null;
            this.fieldsByName = null;
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public HeartbeatSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc) {
        this(type, optional, defaultValue, name, version, doc, null, null, null, null);
    }

    public HeartbeatSchema(Type type) {
        this(type, false, null, null, null, null);
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public String doc() {
        return doc;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fieldsByName.get(fieldName);
    }

    @Override
    public Schema keySchema() {
        if (type != Type.MAP)
            throw new DataException("Cannot look up key schema on non-map type");
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        if (type != Type.MAP && type != Type.ARRAY)
            throw new DataException("Cannot look up value schema on non-array and non-map type");
        return valueSchema;
    }

    public static void validateValue(Schema schema, Object value) {
        if (value == null) {
            if (!schema.isOptional())
                throw new DataException("Invalid value: null used for required field");
            else
                return;
        }

        List<Class> expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.name());

        if (expectedClasses == null)
            expectedClasses = SCHEMA_TYPE_CLASSES.get(schema.type());

        if (expectedClasses == null)
            throw new DataException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());

        boolean foundMatch = false;
        for (Class<?> expectedClass : expectedClasses) {
            if (expectedClass.isInstance(value)) {
                foundMatch = true;
                break;
            }
        }
        if (!foundMatch)
            throw new DataException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    throw new DataException("Struct schemas do not match.");
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                for (Object entry : array)
                    validateValue(schema.valueSchema(), entry);
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.keySchema(), entry.getKey());
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    public void validateValue(Object value) {
        validateValue(this, value);
    }

    @Override
    public HeartbeatSchema schema() {
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeartbeatSchema schema = (HeartbeatSchema) o;
        return Objects.equals(optional, schema.optional) &&
                Objects.equals(type, schema.type) &&
                Objects.deepEquals(defaultValue, schema.defaultValue) &&
                Objects.equals(fields, schema.fields) &&
                Objects.equals(keySchema, schema.keySchema) &&
                Objects.equals(valueSchema, schema.valueSchema) &&
                Objects.equals(name, schema.name) &&
                Objects.equals(version, schema.version) &&
                Objects.equals(doc, schema.doc) &&
                Objects.equals(parameters, schema.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, optional, defaultValue, fields, keySchema, valueSchema, name, version, doc, parameters);
    }

    @Override
    public String toString() {
        if (name != null)
            return "Schema{" + name + ":" + type + "}";
        else
            return "Schema{" + type + "}";
    }

    public static Type schemaType(Class<?> klass) {
        synchronized (JAVA_CLASS_SCHEMA_TYPES) {
            Type schemaType = JAVA_CLASS_SCHEMA_TYPES.get(klass);
            if (schemaType != null)
                return schemaType;

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_SCHEMA_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_SCHEMA_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }
}
