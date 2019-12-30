/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.stream.common.appender.metrics;

import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                reporters.forEach((r) -> logger.info(df.format(new Date()) + " " + r.report()));
            }, 30, 5, TimeUnit.SECONDS);
        }
    }

    public synchronized void shutdown() {
        if (!ses.isShutdown())
            ses.shutdown();
    }
}
