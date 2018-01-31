package com.creditease.dbus.stream.common.appender.metrics;

import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyf on 18/1/11
 */
public class ReporterRegistry {
    private Logger logger = LoggerFactory.getLogger("com.creditease.dbus.metrics");
    private ScheduledExecutorService ses;
    private static ReporterRegistry registry = new ReporterRegistry();
    private Set<DBusMetricReporter> reporters = new ConcurrentHashSet<>();

    public static ReporterRegistry registry() {
        return registry;
    }

    public void register(DBusMetricReporter reporter) {
        reporters.add(reporter);
    }

    private ReporterRegistry() {
        if (logger.isInfoEnabled()) {
            ses = Executors.newScheduledThreadPool(1);
            ses.scheduleAtFixedRate(() -> {
                reporters.forEach((r) -> logger.info(r.report()));
            }, 30, 10, TimeUnit.SECONDS);
        }
    }

    public synchronized void shutdown() {
        if (!ses.isShutdown())
            ses.shutdown();
    }
}
