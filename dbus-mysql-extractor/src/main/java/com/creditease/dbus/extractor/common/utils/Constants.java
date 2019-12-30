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


package com.creditease.dbus.extractor.common.utils;

public class Constants {
    private Constants() {
    }

    public static final String SYS_PROPS_LOG4J_CONFIG = "log4j.configuration";
    public static final String SYS_PROPS_LOG4J2_CONFIG = "log4j.configurationFile";
    public static final String SYS_PROPS_LOG_BASE_PATH = "logs.base.path";
    public static final String SYS_PROPS_LOG_DIR = "log.dir";
    public static final String CONFIG_DB_KEY = "dbus.conf";
    public static final int NEED_ACK_CANAL = 1;
    public static final int NEED_ROLLBACK_CANAL = 2;
    public static final int SEND_NOT_COMPLETED = 3;

    public static final String EXTRACTOR_TOPOLOGY_ID = "extractor.topology.id";
    public static final String ZOOKEEPER_SERVERS = "dbus.zookeeper.servers";
    public static final String EXTRACTOR_ROOT = "/DBus/Extractor";
    public static final String CANAL_ROOT = "/DBus/Canal";
}
