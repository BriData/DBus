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


package com.creditease.dbus.tools.common;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Created by dongwang47 on 2016/11/22.
 */
public abstract class AbstractSignalHandler implements SignalHandler {

    protected volatile boolean running = true;

    public AbstractSignalHandler() {
        // kill命令
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, this);
        // ctrl+c命令
        Signal intSignal = new Signal("INT");
        Signal.handle(intSignal, this);
    }

    @Override
    public void handle(Signal signal) {
        System.out.println("Signal handler called for signal " + signal);
        try {
            System.out.println("politely exiting, please wait 3 seconds...");
            running = false;
        } catch (Exception e) {
            System.out.println("handle|Signal handler" + "failed, reason "
                    + e.getMessage());
            e.printStackTrace();
        }
    }
}
