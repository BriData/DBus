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


package com.creditease.dbus.stream.appender.utils;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
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
