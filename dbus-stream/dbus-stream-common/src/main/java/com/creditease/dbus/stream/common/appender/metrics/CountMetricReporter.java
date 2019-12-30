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
 * Created by zhangyf on 2018/2/12.
 */
public class CountMetricReporter implements DBusMetricReporter {
    protected AtomicLong count;
    protected String meterName;

    protected CountMetricReporter(String meterName) {
        this.meterName = meterName;
        this.count = new AtomicLong(0);
    }

    public static CountMetricReporter create(String meterName) {
        CountMetricReporter reporter = new CountMetricReporter(meterName);
        reporter.register();
        return reporter;
    }

    public void mark(long count) {
        this.count.getAndAdd(count);
    }

    public void mark() {
        this.count.getAndAdd(1);
    }

    @Override
    public String report() {
        return String.format("%-38s spout processing message count: %-15d", meterName, count.get());
    }
}
