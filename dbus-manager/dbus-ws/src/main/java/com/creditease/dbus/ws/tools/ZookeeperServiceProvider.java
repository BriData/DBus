/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.tools;

import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.mgr.base.ConfUtils;
import com.creditease.dbus.ws.common.Constants;

import java.util.Properties;

/**
 * Created by Shrimp on 16/9/10.
 */
public class ZookeeperServiceProvider {
    private IZkService zkService;

    private static class Instance {
        private static ZookeeperServiceProvider provider = new ZookeeperServiceProvider();
    }

    private ZookeeperServiceProvider() {
        try {
            Properties properties = ConfUtils.appConf;
            zkService = new ZkService(properties.getProperty(Constants.ApplicationProp.ZK_SERVERS));
        } catch (Exception e) {
            throw new InitializationException(e);
        }
    }

    public static ZookeeperServiceProvider getInstance() {
        return Instance.provider;
    }

    public IZkService getZkService() {
        return zkService;
    }
}
