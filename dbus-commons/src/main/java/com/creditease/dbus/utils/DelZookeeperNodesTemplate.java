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

public class DelZookeeperNodesTemplate {
    public static String[] ZK_CLEAR_NODES_PATHS = {
            "Canal/canal-placeholder",
            "Extractor/placeholder-mysql-extractor",
            "FullPuller/placeholder",
            "FullPullerGlobal/placeholder",
            "HeartBeat/Monitor/placeholder",
            "Topology/placeholder-appender",
            "Topology/placeholder-dispatcher",
            "Topology/dbus-fulldata-puller/placeholder-fullpuller",
            "Topology/dbus-fulldata-splitter/placeholder-fullsplitter"
    };
    public static String[] ZK_CLEAR_NODES_PATHS_OF_DSNAME_TO_UPPERCASE = {
            "NameSpace/placeholder"
    };
}
