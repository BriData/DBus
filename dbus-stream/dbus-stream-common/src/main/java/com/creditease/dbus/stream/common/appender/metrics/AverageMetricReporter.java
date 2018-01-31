package com.creditease.dbus.stream.common.appender.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangyf on 18/1/12.
 */
public class AverageMetricReporter implements DBusMetricReporter {
    protected AtomicLong total;
    protected AtomicLong count;
    protected String meterName;

    protected AverageMetricReporter(String meterName) {
        this.meterName = meterName;
        this.total = new AtomicLong(0);
        this.count = new AtomicLong(0);
    }

    public static AverageMetricReporter create(String meterName) {
        AverageMetricReporter reporter = new AverageMetricReporter(meterName);
        reporter.register();
        return reporter;
    }

    public void mark(long count) {
        total.getAndAdd(count);
        this.count.incrementAndGet();
    }

    @Override
    public String report() {
        long total = this.total.get();
        long count = this.count.get();
        double avg = 0.0d;
        if (count != 0) {
            avg = (total + 0.0D) / count;
        }
        return String.format("%-38s exec total time MS: %-15d exec count:%-15d avg time MS:%-10f", meterName, total, count, avg);
    }

    public static void main(String[] args) {
        System.out.println(String.format("%-38s exec total time MS: %-15d exec count:%-15d avg time MS:%-10f", "A-ser-time[dispatcher->appender]", 1111, 1111, 0.8838383838));    }
}
