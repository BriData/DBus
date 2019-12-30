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

public class InitZooKeeperNodesTemplate {

    public static String[] ZK_TEMPLATES_NODES_PATHS = {
            "ConfTemplates/Topology/dbus-fulldata-splitter/byte-producer-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/common-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/consumer-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/oracle-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/mysql-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/db2-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-splitter/placeholder-fullsplitter/common-config.properties",

            "ConfTemplates/Topology/dbus-fulldata-puller/byte-producer-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/common-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/consumer-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/oracle-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/mysql-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/db2-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/string-producer-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/hdfs-config.properties",
            "ConfTemplates/Topology/dbus-fulldata-puller/placeholder-fullpuller/common-config.properties",

            "ConfTemplates/Topology/placeholder-appender/configure.properties",
            "ConfTemplates/Topology/placeholder-appender/consumer-config.properties",
            "ConfTemplates/Topology/placeholder-appender/ora-meta.properties",
            "ConfTemplates/Topology/placeholder-appender/db2-meta.properties",
            "ConfTemplates/Topology/placeholder-appender/producer-config.properties",
            "ConfTemplates/Topology/placeholder-appender/producer-control.properties",

            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.configure.properties",
            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.consumer.properties",
            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.db2.consumer.properties",
            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.producer.properties",
            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.raw.topics.properties",
            "ConfTemplates/Topology/placeholder-dispatcher/dispatcher.schema.topics.properties",

            "ConfTemplates/Topology/placeholder-log-processor/config.properties",
            "ConfTemplates/Topology/placeholder-log-processor/consumer.properties",
            "ConfTemplates/Topology/placeholder-log-processor/producer.properties",

            "ConfTemplates/Extractor/placeholder-mysql-extractor/jdbc.properties",
            "ConfTemplates/Extractor/placeholder-mysql-extractor/config.properties",
            "ConfTemplates/Extractor/placeholder-mysql-extractor/producer.properties",
            "ConfTemplates/Extractor/placeholder-mysql-extractor/consumer.properties",

            "ConfTemplates/Extractor/placeholder-mongo-extractor/config.properties",
            "ConfTemplates/Extractor/placeholder-mongo-extractor/producer.properties",
            "ConfTemplates/Extractor/placeholder-mongo-extractor/consumer.properties",

            "ConfTemplates/Router/placeholder-router/config.properties",
            "ConfTemplates/Router/placeholder-router/consumer.properties",
            "ConfTemplates/Router/placeholder-router/producer.properties",

            "ConfTemplates/Sinker/placeholder-sinker/config.properties",
            "ConfTemplates/Sinker/placeholder-sinker/consumer.properties",
            "ConfTemplates/Sinker/placeholder-sinker/producer.properties",
            "ConfTemplates/Sinker/placeholder-sinker/hdfs-config.properties"

    };
    public static String[] ZK_OTHER_NODES_PATHS = {
            "Commons/mysql.properties",

            "HeartBeat/Config/heartbeat_config.json",

            "Keeper/consumer.properties",
            "Keeper/ctlmsg-producer.properties",
            "Keeper/producer.properties",
            "Keeper/project-config.properties"
    };

    public static String[] ZK_EMPTY_NODES_PATHS = {
            "Streaming",
            "ControlMessageResult",
            "FullPuller",
            "FullPuller/Projects",
            "FullPullerGlobal",
            "Canal",
            "NameSpace",
            "Extractor",
            "Topology",
            "Router",
            "Sinker",
            "HeartBeat/ProjectMonitor",
            "HeartBeat/Control",
            "Commons/global.properties",
            "ConfTemplates/Extractor/placeholder-mysql-extractor/filter.properties",
            "Commons/auto-deploy-ogg.conf",
            "Commons/auto-deploy-canal.conf"
    };

}
