package com.creditease.dbus.heartbeat.distributed.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.creditease.dbus.heartbeat.distributed.errors.ConfigException;
import com.creditease.dbus.heartbeat.distributed.utils.Utils;

import org.apache.commons.lang3.StringUtils;

public class ConfigDef {

    public static final Object NO_DEFAULT_VALUE = new String("");

    private final Map<String, ConfigKey> configKeys = new HashMap<>();
    private final List<String> groups = new LinkedList<>();
    private Set<String> configsWithNoParent;

    public Set<String> names() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        if (configKeys.containsKey(name)) {
            throw new ConfigException("Configuration " + name + " is defined twice.");
        }
        if (group != null && !groups.contains(group)) {
            groups.add(group);
        }
        Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
        configKeys.put(name, new ConfigKey(name, type, parsedDefault, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender));
        return this;
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation) {
        return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
    }

    /**
     * Get the configuration keys
     * @return a map containing all configuration keys
     */
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    /**
     * Get the groups for the configuration
     * @return a list of group names
     */
    public List<String> groups() {
        return groups;
    }

    public Map<String, Object> parse(Map<?, ?> props) {
        // Check all configurations are defined
        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        if (!undefinedConfigKeys.isEmpty()) {
            String joined = Utils.join(undefinedConfigKeys, ",");
            throw new ConfigException("Some configurations in are referred in the dependents, but not defined: " + joined);
        }
        // parse all known keys
        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            // props map contains setting - assign ConfigKey value
            if (props.containsKey(key.name)) {
                value = parseType(key.name, props.get(key.name), key.type);
                // props map doesn't contain setting, the key is required because no default value specified - its an error
            } else if (key.defaultValue == NO_DEFAULT_VALUE) {
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            } else {
                // otherwise assign setting its default value
                value = key.defaultValue;
            }
            if (key.validator != null) {
                key.validator.ensureValid(key.name, value);
            }
            values.put(key.name, value);
        }
        return values;
    }

    Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
        Map<String, Object> parsed = new HashMap<>();
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            parseForValidate(name, props, parsed, configValues);
        }
        return parsed;
    }


    private List<ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            validate(name, parsed, configValues);
        }
        return new LinkedList<>(configValues.values());
    }

    private List<String> undefinedDependentConfigs() {
        Set<String> undefinedConfigKeys = new HashSet<>();
        for (String configName: configKeys.keySet()) {
            ConfigKey configKey = configKeys.get(configName);
            List<String> dependents = configKey.dependents;
            for (String dependent: dependents) {
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent);
                }
            }
        }
        return new LinkedList<>(undefinedConfigKeys);
    }

    private Set<String> getConfigsWithNoParent() {
        if (this.configsWithNoParent != null) {
            return this.configsWithNoParent;
        }
        Set<String> configsWithParent = new HashSet<>();

        for (ConfigKey configKey: configKeys.values()) {
            List<String> dependents = configKey.dependents;
            configsWithParent.addAll(dependents);
        }

        Set<String> configs = new HashSet<>(configKeys.keySet());
        configs.removeAll(configsWithParent);
        this.configsWithNoParent = configs;
        return configs;
    }

    public List<ConfigValue> validate(Map<String, String> props) {
        Map<String, ConfigValue> configValues = new HashMap<>();
        for (String name: configKeys.keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        for (String undefinedConfigKey: undefinedConfigKeys) {
            ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
            undefinedConfigValue.addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValues.put(undefinedConfigKey, undefinedConfigValue);
        }

        Map<String, Object> parsed = parseForValidate(props, configValues);
        return validate(parsed, configValues);
    }

    private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);

        Object value = null;
        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props.get(key.name), key.type);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        } else if (key.defaultValue == NO_DEFAULT_VALUE) {
            config.addErrorMessage("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            value = key.defaultValue;
        }

        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }
        config.value(value);
        parsed.put(name, value);
        for (String dependent: key.dependents) {
            parseForValidate(dependent, props, parsed, configs);
        }
    }

    private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);
        List<Object> recommendedValues;
        if (key.recommender != null) {
            try {
                recommendedValues = key.recommender.validValues(name, parsed);
                List<Object> originalRecommendedValues = config.recommendedValues();
                if (!originalRecommendedValues.isEmpty()) {
                    Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
                    Iterator<Object> it = recommendedValues.iterator();
                    while (it.hasNext()) {
                        Object o = it.next();
                        if (!originalRecommendedValueSet.contains(o)) {
                            it.remove();
                        }
                    }
                }
                config.recommendedValues(recommendedValues);
                config.visible(key.recommender.visible(name, parsed));
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }

        configs.put(name, config);
        for (String dependent: key.dependents) {
            validate(dependent, parsed, configs);
        }
    }

    public static String convertToString(Object parsedValue, org.apache.kafka.common.config.ConfigDef.Type type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case STRING:
            case PASSWORD:
                return parsedValue.toString();
            case LIST:
                List<?> valueList = (List<?>) parsedValue;
                return Utils.join(valueList, ",");
            case CLASS:
                Class<?> clazz = (Class<?>) parsedValue;
                return clazz.getCanonicalName();
            default:
                throw new IllegalStateException("Unknown type.");
        }
    }

    private Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) return null;

            String trimmed = null;
            if (value instanceof String)
                trimmed = ((String) value).trim();

            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true"))
                            return true;
                        else if (trimmed.equalsIgnoreCase("false"))
                            return false;
                        else
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                    } else if (value instanceof Boolean)
                        return value;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                /*case PASSWORD:
                    if (value instanceof Password)
                        return value;
                    else if (value instanceof String)
                        return new Password(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());*/
                case STRING:
                    if (value instanceof String)
                        return trimmed;
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case INT:
                    if (value instanceof Integer) {
                        return (Integer) value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return (Short) value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case LONG:
                    if (value instanceof Integer)
                        return ((Integer) value).longValue();
                    if (value instanceof Long)
                        return (Long) value;
                    else if (value instanceof String)
                        return Long.parseLong(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case DOUBLE:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof String)
                        return Double.parseDouble(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case LIST:
                    if (value instanceof List)
                        return (List<?>) value;
                    else if (value instanceof String)
                        if (trimmed.isEmpty())
                            return Collections.emptyList();
                        else
                            return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
                    else
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                case CLASS:
                    if (value instanceof Class)
                        return (Class<?>) value;
                    else if (value instanceof String)
                        return Class.forName(trimmed, true, com.creditease.dbus.heartbeat.distributed.utils.Utils.getClassLoader());
                    else
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    public static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;
        public final String group;
        public final int orderInGroup;
        public final Width width;
        public final String displayName;
        public final List<String> dependents;
        public final Recommender recommender;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator,
                         Importance importance, String documentation, String group,
                         int orderInGroup, Width width, String displayName,
                         List<String> dependents, Recommender recommender) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && this.hasDefault())
                this.validator.ensureValid(name, defaultValue);
            this.documentation = documentation;
            this.dependents = dependents;
            this.group = group;
            this.orderInGroup = orderInGroup;
            this.width = width;
            this.displayName = displayName;
            this.recommender = recommender;
        }

        public boolean hasDefault() {
            return this.defaultValue != NO_DEFAULT_VALUE;
        }
    }

    /**
     * The config types
     */
    public enum Type {
        BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    }

    /**
     * The importance level for a configuration
     */
    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    /**
     * The width of a configuration value
     */
    public enum Width {
        NONE, SHORT, MEDIUM, LONG
    }

    public interface Recommender {

        List<Object> validValues(String name, Map<String, Object> parsedConfig);

        boolean visible(String name, Map<String, Object> parsedConfig);
    }

    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    public interface Validator {
        /**
         * Perform single configuration validation.
         * @param name The name of the configuration
         * @param value The value of the configuration
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        /**
         * A numeric range that checks both the upper and lower bound
         */
        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new ConfigException(name, o, "Value must be non-null");
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue())
                throw new ConfigException(name, o, "Value must be at least " + min);
            if (max != null && n.doubleValue() > max.doubleValue())
                throw new ConfigException(name, o, "Value must be no more than " + max);
        }

        public String toString() {
            if (min == null)
                return "[...," + max + "]";
            else if (max == null)
                return "[" + min + ",...]";
            else
                return "[" + min + ",...," + max + "]";
        }
    }

    public static class ValidString implements Validator {
        List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + StringUtils.join(validStrings, ", "));
            }

        }

        public String toString() {
            return "[" + StringUtils.join(validStrings, ", ") + "]";
        }
    }

    protected List<String> headers() {
        return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", "Importance");
    }

    protected String getConfigValue(ConfigKey key, String headerName) {
        switch (headerName) {
            case "Name":
                return key.name;
            case "Description":
                return key.documentation;
            case "Type":
                return key.type.toString().toLowerCase(Locale.ROOT);
            case "Default":
                if (key.hasDefault()) {
                    if (key.defaultValue == null)
                        return "null";
                    else if (key.type == Type.STRING && key.defaultValue.toString().isEmpty())
                        return "\"\"";
                    else
                        return key.defaultValue.toString();
                } else
                    return "";
            case "Valid Values":
                return key.validator != null ? key.validator.toString() : "";
            case "Importance":
                return key.importance.toString().toLowerCase(Locale.ROOT);
            default:
                throw new RuntimeException("Can't find value for header '" + headerName + "' in " + key.name);
        }
    }

    public String toHtmlTable() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        // print column headers
        for (String headerName : headers()) {
            b.append("<th>");
            b.append(headerName);
            b.append("</th>\n");
        }
        b.append("</tr>\n");
        for (ConfigKey def : configs) {
            b.append("<tr>\n");
            // print column values
            for (String headerName : headers()) {
                b.append("<td>");
                b.append(getConfigValue(def, headerName));
                b.append("</td>");
            }
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    /**
     * Get the configs formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toRst() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();

        for (ConfigKey def : configs) {
            b.append("``");
            b.append(def.name);
            b.append("``\n");
            for (String docLine : def.documentation.split("\n")) {
                if (docLine.length() == 0) {
                    continue;
                }
                b.append("  ");
                b.append(docLine);
                b.append("\n\n");
            }
            b.append("  * Type: ");
            b.append(def.type.toString().toLowerCase(Locale.ROOT));
            b.append("\n");
            if (def.defaultValue != null) {
                b.append("  * Default: ");
                if (def.type == Type.STRING) {
                    b.append("\"");
                    b.append(def.defaultValue);
                    b.append("\"");
                } else {
                    b.append(def.defaultValue);
                }
                b.append("\n");
            }
            b.append("  * Importance: ");
            b.append(def.importance.toString().toLowerCase(Locale.ROOT));
            b.append("\n\n");
        }
        return b.toString();
    }

    /**
     * Get a list of configs sorted into "natural" order: listing required fields first, then
     * ordering by importance, and finally by name.
     */
    protected List<ConfigKey> sortedConfigs() {
        // sort first required fields, then by importance, then name
        List<ConfigKey> configs = new ArrayList<>(this.configKeys.values());
        Collections.sort(configs, new Comparator<ConfigKey>() {
            public int compare(ConfigKey k1, ConfigKey k2) {
                // first take anything with no default value
                if (!k1.hasDefault() && k2.hasDefault()) {
                    return -1;
                } else if (!k2.hasDefault() && k1.hasDefault()) {
                    return 1;
                }

                // then sort by importance
                int cmp = k1.importance.compareTo(k2.importance);
                if (cmp == 0) {
                    // then sort in alphabetical order
                    return k1.name.compareTo(k2.name);
                } else {
                    return cmp;
                }
            }
        });
        return configs;
    }

}
