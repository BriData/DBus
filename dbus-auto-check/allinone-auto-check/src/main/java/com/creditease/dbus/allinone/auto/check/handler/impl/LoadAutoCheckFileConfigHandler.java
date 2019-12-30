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


package com.creditease.dbus.allinone.auto.check.handler.impl;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.resource.IResource;
import com.creditease.dbus.allinone.auto.check.resource.local.AutoCheckFileConfigResource;

import java.io.BufferedWriter;

public class LoadAutoCheckFileConfigHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
        loadAutoCheckConfig();
    }

    private void loadAutoCheckConfig() {
        IResource<AutoCheckConfigBean> resource = new AutoCheckFileConfigResource("auto-check-conf.properties");
        AutoCheckConfigBean conf = resource.load();
        AutoCheckConfigContainer.getInstance().setAutoCheckConf(conf);
    }

}
