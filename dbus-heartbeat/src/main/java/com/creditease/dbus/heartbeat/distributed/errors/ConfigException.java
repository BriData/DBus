package com.creditease.dbus.heartbeat.distributed.errors;

public class ConfigException extends  HeartbeatException {

    public ConfigException(String name, Object value) {
        this(name, value, null);
    }

    public ConfigException(String name, Object value, String message) {
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }

    public ConfigException(String s) {
        super(s);
    }

    public ConfigException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConfigException(Throwable throwable) {
        super(throwable);
    }

}
