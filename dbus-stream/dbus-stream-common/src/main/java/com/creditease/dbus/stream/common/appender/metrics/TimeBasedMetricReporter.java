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
