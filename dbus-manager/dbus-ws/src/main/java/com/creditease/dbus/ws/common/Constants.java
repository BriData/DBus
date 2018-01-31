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

package com.creditease.dbus.ws.common;

/**
 * Created by Shrimp on 16/9/10.
 */
public class Constants {
    public static class ApplicationProp {
        public static final String ZK_SERVERS = "zk.servers";
        public static final String KAFKA_SERVERS = "kafka.bootstrap.servers";
    }

    /**
     * 拉全量过程中,zookeeper相关表的节点的状态常量
     */
    public static class NodeStatus {
        public static final String WAITING = "waiting";
        public static final String RUNNING = "running";
        public static final String ENDING = "ending";
    }
//    public static final String DBUS_ROOT ="/DBus";
//    public static final String  TEMPLATE_NODE_NAME = "/ConfTemplates";
//    public static final String TEMPLATE_ROOT="/DBus/ConfTemplates";
    public static final String BUSSINESS_PLACEHOLDER="placeholder";
    public static final String CONTROL_MESSAGE_SENDER_NAME = "dbus-rest";
    public static final String GLOBAL_CONF = "/DBus/Commons/global.properties";
}
