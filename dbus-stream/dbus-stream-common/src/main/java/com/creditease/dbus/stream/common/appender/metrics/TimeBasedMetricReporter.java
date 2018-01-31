package com.creditease.dbus.stream.common.appender.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangyf on 18/1/10.
 */
public class TimeBasedMetricReporter extends AverageMetricReporter {
    private AtomicLong startTimeMS;

    private TimeBasedMetricReporter(String meterName) {
        super(meterName);
        this.startTimeMS = new AtomicLong(0);
    }

    public static TimeBasedMetricReporter create(String meterName) {
        TimeBasedMetricReporter reporter = new TimeBasedMetricReporter(meterName);
        reporter.register();
        return reporter;
    }

    public void start() {
        startTimeMS.getAndSet(System.currentTimeMillis());
    }

    public void stop() {
        mark(System.currentTimeMillis() - startTimeMS.get());
    }
}
