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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.heartbeat;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat.HeartbeatDefaultHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.HeartbeatHandlerListener;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class OracleHeartbeatHandler extends HeartbeatDefaultHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public OracleHeartbeatHandler(HeartbeatHandlerListener listener) {
        super(listener);
    }

    protected String generateUmsId(String pos) {
        try {
            Properties properties = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
            Long compensation = Long.parseLong(properties.getOrDefault(Constants.ConfigureKey.LOGFILE_NUM_COMPENSATION, 0).toString());
            if (compensation == null || compensation == 0) {
                return pos;
            }
            logger.debug("heartbeat logfile.number.compensation:{}", compensation);
            String umsId = Utils.oracleUMSID(pos, compensation);
            return umsId;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
