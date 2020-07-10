package com.creditease.dbus.heartbeat.distributed.config;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ConfigValue {

    private String name;
    private Object value;
    private List<Object> recommendedValues;
    private List<String> errorMessages;
    private boolean visible;

    public ConfigValue(String name) {
        this(name, null, new LinkedList<Object>(), new LinkedList<String>());
    }

    public ConfigValue(String name, Object value, List<Object> recommendedValues, List<String> errorMessages) {
        this.name = name;
        this.value = value;
        this.recommendedValues = recommendedValues;
        this.errorMessages = errorMessages;
        this.visible = true;
    }

    public String name() {
        return name;
    }

    public Object value() {
        return value;
    }

    public List<Object> recommendedValues() {
        return recommendedValues;
    }

    public List<String> errorMessages() {
        return errorMessages;
    }

    public boolean visible() {
        return visible;
    }

    public void value(Object value) {
        this.value = value;
    }

    public void recommendedValues(List<Object> recommendedValues) {
        this.recommendedValues = recommendedValues;
    }

    public void addErrorMessage(String errorMessage) {
        this.errorMessages.add(errorMessage);
    }

    public void visible(boolean visible) {
        this.visible = visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigValue that = (ConfigValue) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value) &&
                Objects.equals(recommendedValues, that.recommendedValues) &&
                Objects.equals(errorMessages, that.errorMessages) &&
                Objects.equals(visible, that.visible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, recommendedValues, errorMessages, visible);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
                .append(name)
                .append(",")
                .append(value)
                .append(",")
                .append(recommendedValues)
                .append(",")
                .append(errorMessages)
                .append(",")
                .append(visible)
                .append("]");
        return sb.toString();
    }

}
