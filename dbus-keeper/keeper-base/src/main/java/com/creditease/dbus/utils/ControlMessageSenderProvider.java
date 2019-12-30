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


package com.creditease.dbus.utils;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;

public class ControlMessageSenderProvider {

    public static ControlMessageSender getControlMessageSender(IZkService zkService) {
        try {
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            Properties producerConf = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
            producerConf.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                producerConf.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            return new ControlMessageSender(producerConf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
