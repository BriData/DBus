package com.creditease.dbus.stream.common.appender.metrics;

/**
 * Created by zhangyf on 18/1/12.
 */
public interface DBusMetricReporter {
    String report();
    default void register() {
        ReporterRegistry.registry().register(this);
    }
}
