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


package com.creditease.dbus.log.container;

import com.creditease.dbus.log.bean.LogCheckConfigBean;


public class AutoCheckConfigContainer {

    private static AutoCheckConfigContainer container;

    private LogCheckConfigBean logCheckConf;

    private AutoCheckConfigContainer() {
    }

    public static AutoCheckConfigContainer getInstance() {
        if (container == null) {
            synchronized (AutoCheckConfigContainer.class) {
                if (container == null)
                    container = new AutoCheckConfigContainer();
            }
        }
        return container;
    }

    public LogCheckConfigBean getAutoCheckConf() {
        return logCheckConf;
    }

    public void setAutoCheckConf(LogCheckConfigBean logCheckConf) {
        this.logCheckConf = logCheckConf;
    }

    public void clear() {
        logCheckConf = null;
    }
}
