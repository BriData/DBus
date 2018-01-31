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

import com.creditease.dbus.mgr.base.ConfUtils;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.ws.common.Constants;

import java.util.Properties;

/**
 * Created by Shrimp on 16/9/9.
 */
public class ControlMessageSenderProvider {

    private static class Instance {
        private static ControlMessageSenderProvider provider = new ControlMessageSenderProvider();
    }

    public static ControlMessageSenderProvider getInstance() {
        return Instance.provider;
    }

    public ControlMessageSender getSender() throws Exception {
        return new ControlMessageSender(buildConf());
    }

    private static Properties buildConf() {
        try {
            ZookeeperServiceProvider zk = ZookeeperServiceProvider.getInstance();
            Properties globalConf = zk.getZkService().getProperties(Constants.GLOBAL_CONF);

            Properties producerConf = new Properties();
            producerConf.putAll(ConfUtils.ctlmsgProducerConf);

            //producerConf.setProperty("client.id", Constants.CONTROL_MESSAGE_SENDER_NAME);
            producerConf.setProperty("bootstrap.servers", globalConf.getProperty("bootstrap.servers"));
            return producerConf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
