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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.event.IEvent;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

public class LifeCycleContainer {

    private static LifeCycleContainer container;

    private ConcurrentHashMap<IEvent, Thread> cmp = new ConcurrentHashMap<IEvent, Thread>();

    private LifeCycleContainer() {
    }

    public static LifeCycleContainer getInstance() {
        if (container == null) {
            synchronized (LifeCycleContainer.class) {
                if (container == null)
                    container = new LifeCycleContainer();
            }
        }
        return container;
    }

    public void put(IEvent key, Thread value) {
        cmp.put(key, value);
    }

    public void stop() {
        Enumeration<IEvent> keys = cmp.keys();
        while (keys.hasMoreElements()) {
            IEvent event = keys.nextElement();
            Thread thread = cmp.get(event);
            event.stop();
            thread.interrupt();
        }
    }

    public void clear() {
        cmp.clear();
    }

}
