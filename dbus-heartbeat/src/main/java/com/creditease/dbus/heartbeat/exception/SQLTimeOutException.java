package com.creditease.dbus.heartbeat.exception;

/**
 * Created by Administrator on 2017/11/15.
 */
public class SQLTimeOutException extends RuntimeException {
    public SQLTimeOutException() {
        super();
    }

    public SQLTimeOutException(String message) {
        super(message);
    }

    public SQLTimeOutException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLTimeOutException(Throwable cause) {
        super(cause);
    }

    protected SQLTimeOutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
