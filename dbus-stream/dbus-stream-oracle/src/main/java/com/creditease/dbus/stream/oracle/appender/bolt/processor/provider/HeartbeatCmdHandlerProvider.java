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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.provider;

import avro.shaded.com.google.common.collect.Maps;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat.HeartbeatDefaultHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.heartbeat.HeartbeatReloadHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.HeartbeatHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by zhangyf on 17/8/16.
 */
public class HeartbeatCmdHandlerProvider implements BoltCommandHandlerProvider {
    private Logger logger = LoggerFactory.getLogger(getClass());
    protected HeartbeatHandlerListener listener;

    public HeartbeatCmdHandlerProvider(HeartbeatHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public Map<Command, BoltCommandHandler> provideHandlers(DbusDatasourceType type) {
        Map<Command, BoltCommandHandler> map = Maps.newHashMap();
        map.put(Command.APPENDER_RELOAD_CONFIG, new HeartbeatReloadHandler(listener));
        return map;
    }

    @Override
    public BoltCommandHandler provideDefaultHandler(DbusDatasourceType type) {
        return new HeartbeatDefaultHandler(listener) {
            protected String generateUmsId(String pos) {
                try {
                    Properties properties = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
                    Long compensation = Long.parseLong(properties.getOrDefault(Constants.ConfigureKey.LOGFILE_NUM_COMPENSATION, 0).toString());
                    String umsId = Utils.oracleUMSID(pos, compensation);
                    logger.debug("logfile.number.compensation:{}", compensation);
                    return umsId;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
