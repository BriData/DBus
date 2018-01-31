package com.creditease.dbus.stream.appender.utils;

import org.apache.storm.shade.com.codahale.metrics.Meter;
import org.apache.storm.shade.com.codahale.metrics.MetricRegistry;
import org.apache.storm.shade.com.codahale.metrics.ScheduledReporter;
import org.apache.storm.shade.com.codahale.metrics.Slf4jReporter;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyf on 18/1/10.
 */
public class SingleMetricReporter {
    private MetricRegistry metrics = new MetricRegistry();
    private ScheduledReporter reporter = null;
    private Meter countMeter = null;

    public SingleMetricReporter(String logName, String meterName, int seconds) {
        reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger(logName))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        countMeter = metrics.meter(meterName);
        reporter.start(seconds, TimeUnit.SECONDS);
    }

    public void report(long count) {
        countMeter.mark(count);
    }
}
