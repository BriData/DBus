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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Shrimp on 16/7/11.
 */
public class AppenderMetricReporter {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private MetricRegistry metrics = new MetricRegistry();
    private ScheduledReporter reporter = null;
    private Meter messagesMeter = null;
    private Meter bytesMeter = null;
    private static AppenderMetricReporter instance = new AppenderMetricReporter();

    public static AppenderMetricReporter getInstance() {
        return instance;
    }

    private AppenderMetricReporter() {
        reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger("com.creditease.dbus.metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        messagesMeter = metrics.meter("A-kafka_messages");
        bytesMeter = metrics.meter("A-Bytes");
        reporter.start(10, TimeUnit.SECONDS);
    }

    public void report(int messageSize, int count) {
        bytesMeter.mark(messageSize);
        messagesMeter.mark(count);
    }
}
