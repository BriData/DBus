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


package com.creditease.dbus.log.handler.impl;


import com.creditease.dbus.log.bean.LogCheckConfigBean;
import com.creditease.dbus.log.container.AutoCheckConfigContainer;
import com.creditease.dbus.log.handler.AbstractHandler;
import com.creditease.dbus.log.resource.IResource;
import com.creditease.dbus.log.resource.local.AutoCheckFileConfigResource;

import java.io.BufferedWriter;

public class LoadConfigFileHandler extends AbstractHandler {

    @Override
    public void checkDeploy(BufferedWriter bw) throws Exception {
        loadAutoCheckConfig();
    }

    private void loadAutoCheckConfig() {
        IResource<LogCheckConfigBean> resource = new AutoCheckFileConfigResource("log-conf.properties");
        LogCheckConfigBean conf = resource.load();
        AutoCheckConfigContainer.getInstance().setAutoCheckConf(conf);
    }

}
