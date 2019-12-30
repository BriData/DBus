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


package com.creditease.dbus.heartbeat.type;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayItem implements Delayed {

    // ms
    private long trigger;

    private String schema;

    public DelayItem(long delayMs, String schema) {
        trigger = System.currentTimeMillis() + delayMs;
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public int compareTo(Delayed o) {
        int ret = 0;
        DelayItem target = (DelayItem) o;
        if (this.trigger - target.trigger > 0) {
            ret = 1;
        } else if (this.trigger - target.trigger < 0) {
            ret = -1;
        }
        return ret;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return trigger - System.currentTimeMillis();
    }

}
