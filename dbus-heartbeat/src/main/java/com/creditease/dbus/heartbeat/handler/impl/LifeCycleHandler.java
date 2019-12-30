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


package com.creditease.dbus.heartbeat.handler.impl;

import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.LifeCycleContainer;
import com.creditease.dbus.heartbeat.event.IEvent;
import com.creditease.dbus.heartbeat.event.impl.LifeEvent;
import com.creditease.dbus.heartbeat.handler.AbstractHandler;

public class LifeCycleHandler extends AbstractHandler {

    @Override
    public void process() {
        long lifeInterval = HeartBeatConfigContainer.getInstance().getHbConf().getLifeInterval();
        IEvent lifeEvent = new LifeEvent(lifeInterval);
        Thread let = new Thread(lifeEvent, "life-event");
        let.start();
        LifeCycleContainer.getInstance().put(lifeEvent, let);
    }

}
