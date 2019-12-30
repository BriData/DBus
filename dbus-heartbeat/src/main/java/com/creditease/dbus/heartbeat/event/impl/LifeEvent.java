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


package com.creditease.dbus.heartbeat.event.impl;

import com.creditease.dbus.heartbeat.event.AbstractEvent;

import java.util.concurrent.TimeUnit;

public class LifeEvent extends AbstractEvent {

    public LifeEvent(long interval) {
        super(interval);
    }

    @Override
    public void run() {
        while (isRun.get()) {
            try {
                sleep(interval, TimeUnit.SECONDS);
                LOG.debug("[life-event] heart beat生命心跳.");
            } catch (Exception e) {
                LOG.error("[life-event]", e);
            }
        }
    }

}
