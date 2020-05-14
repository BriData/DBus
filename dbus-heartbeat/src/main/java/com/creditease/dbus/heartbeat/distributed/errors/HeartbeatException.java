package com.creditease.dbus.heartbeat.distributed.errors;

public class HeartbeatException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public HeartbeatException(String message, Throwable cause) {
        super(message, cause);
    }

    public HeartbeatException(String message) {
        super(message);
    }

    public HeartbeatException(Throwable cause) {
        super(cause);
    }

    public HeartbeatException() {
        super();
    }

}
