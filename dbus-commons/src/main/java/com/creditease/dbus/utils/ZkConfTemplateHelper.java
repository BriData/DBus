/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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
import com.creditease.dbus.enums.DbusDatasourceType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Shrimp on 16/8/19.
 */
public class ZkConfTemplateHelper {

    public static String[] ZK_CONF_PATHS = {
            "Topology/dbus-fulldata-splitter/byte-producer-config.properties",
            "Topology/dbus-fulldata-splitter/common-config.properties",
            "Topology/dbus-fulldata-splitter/consumer-config.properties",
            "Topology/dbus-fulldata-splitter/oracle-config.properties",
            "Topology/dbus-fulldata-splitter/string-producer-config.properties",
            "Topology/dbus-fulldata-splitter/placeholder-fullsplitter/common-config.properties",

            "Topology/dbus-fulldata-puller/byte-producer-config.properties",
            "Topology/dbus-fulldata-puller/common-config.properties",
            "Topology/dbus-fulldata-puller/consumer-config.properties",
            "Topology/dbus-fulldata-puller/oracle-config.properties",
            "Topology/dbus-fulldata-puller/string-producer-config.properties",
            "Topology/dbus-fulldata-puller/placeholder-fullpuller/common-config.properties",

            "Topology/placeholder-appender/configure.properties",
            "Topology/placeholder-appender/consumer-config.properties",
            "Topology/placeholder-appender/ora-meta.properties",
            "Topology/placeholder-appender/producer-config.properties",
            "Topology/placeholder-appender/producer-control.properties",

            "Topology/placeholder-dispatcher/dispatcher.configure.properties",
            "Topology/placeholder-dispatcher/dispatcher.consumer.properties",
            "Topology/placeholder-dispatcher/dispatcher.producer.properties",
            "Topology/placeholder-dispatcher/dispatcher.raw.topics.properties",
            "Topology/placeholder-dispatcher/dispatcher.schema.topics.properties",

            "Topology/placeholder-log-processor/config.properties",
            "Topology/placeholder-log-processor/consumer.properties",
            "Topology/placeholder-log-processor/producer.properties",

            "Extractor/placeholder-mysql-extractor/jdbc.properties",
            "Extractor/placeholder-mysql-extractor/config.properties",
            "Extractor/placeholder-mysql-extractor/producer.properties",
            "Extractor/placeholder-mysql-extractor/consumer.properties"
    };

    private static Map<String, Set> zkConfTemplateRootPathsOfDsType = new HashMap<>();

    static {
        Set<String> dbBaseZkConfRootNodes = new HashSet<>();
        dbBaseZkConfRootNodes.add("Topology/" + Constants.FULL_SPLITTING_PROPS_ROOT);
        dbBaseZkConfRootNodes.add("Topology/" + Constants.FULL_PULLING_PROPS_ROOT);
        dbBaseZkConfRootNodes.add("Topology/" + "placeholder-appender");
        dbBaseZkConfRootNodes.add("Topology/" + "placeholder-dispatcher");

        //mysql extractor模板
        Set<String> mysqlZkConfRootNodes = new HashSet<>();
        mysqlZkConfRootNodes.addAll(dbBaseZkConfRootNodes);
        mysqlZkConfRootNodes.add("Extractor/placeholder-mysql-extractor");

        //mongo extractor模板
        Set<String> mongoZkConfRootNodes = new HashSet<>();
        mongoZkConfRootNodes.addAll(dbBaseZkConfRootNodes);
        mongoZkConfRootNodes.add("Extractor/placeholder-mongo-extractor");

        Set<String> logZkConfRootNodes = new HashSet<>();
        logZkConfRootNodes.add("Topology/" + "placeholder-log-processor");

        zkConfTemplateRootPathsOfDsType.put(DbusDatasourceType.ORACLE.name().toLowerCase(), dbBaseZkConfRootNodes);
        zkConfTemplateRootPathsOfDsType.put(DbusDatasourceType.MONGO.name().toLowerCase(), mongoZkConfRootNodes);
        zkConfTemplateRootPathsOfDsType.put(DbusDatasourceType.MYSQL.name().toLowerCase(), mysqlZkConfRootNodes);
        // log类共享同一个模板
        zkConfTemplateRootPathsOfDsType.put(DbusDatasourceType.ALIAS_FOR_ALL_LOG_DS_TYPE.toLowerCase(), logZkConfRootNodes);
    }

    public static Set<String> getZkConfRootNodesSetForDs(DbusDatasourceType dsType) {
        return zkConfTemplateRootPathsOfDsType.get(DbusDatasourceType.getAliasOfDsType(dsType));
    }
}
