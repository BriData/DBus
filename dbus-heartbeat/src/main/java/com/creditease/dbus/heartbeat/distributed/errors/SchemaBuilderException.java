package com.creditease.dbus.heartbeat.distributed.errors;

public class SchemaBuilderException extends DataException {

    public SchemaBuilderException(String s) {
        super(s);
    }

    public SchemaBuilderException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public SchemaBuilderException(Throwable throwable) {
        super(throwable);
    }

}
