package com.creditease.dbus.heartbeat.distributed.errors;

public class DataException extends  HeartbeatException {

    public DataException(String s) {
        super(s);
    }

    public DataException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DataException(Throwable throwable) {
        super(throwable);
    }

}
