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

package com.creditease.dbus.constant;

/**
 * Created by zhangyf on 2018/3/21.
 */
public class KeeperConstants {

    public static final String USER_ROLE_TYPE_ADMIN = "admin";
    public static final String USER_ROLE_TYPE_APP = "app";
    public static final String USER_ROLE_TYPE_USER = "user";

    public static final String PARAMETER_KEY_ROLE = "__role__";
    public static final String PARAMETER_KEY_USER = "__user_id__";
    public static final String PARAMETER_KEY_EMAIL = "__user_email__";

    public static class ApplicationProp {
        public static final String ZK_SERVERS = "zk.servers";
        public static final String KAFKA_SERVERS = "kafka.bootstrap.servers";
    }

    /**
     * zookeeper properties name
     */

    public static final String BUSSINESS_PLACEHOLDER = "placeholder";
    public static final String CONTROL_MESSAGE_SENDER_NAME = "dbus-rest";

    public static final String GLOBAL_CONF_KEY_STORM_START_SCRIPT_PATH = "stormStartScriptPath";
    public static final String GLOBAL_CONF_KEY_STORM_SSH_USER = "user";
    public static final String GLOBAL_CONF_KEY_STORM_REST_API = "stormRest";
    public static final String GLOBAL_CONF_KEY_STORM_NIMBUS_HOST = "storm.nimbus.host";
    public static final String GLOBAL_CONF_KEY_STORM_NIMBUS_PORT = "storm.nimbus.port";
    public static final String GLOBAL_CONF_KEY_STORM_HOME_PATH = "storm.home.path";
    public static final String GLOBAL_CONF_KEY_GRAFANA_URL = "grafana_url_web";
    public static final String GLOBAL_CONF_KEY_GRAFANA_URL_DBUS = "grafana_url_dbus";
    public static final String GLOBAL_CONF_KEY_INFLUXDB_URL = "influxdb_url_web";
    public static final String GLOBAL_CONF_KEY_INFLUXDB_URL_DBUS = "influxdb_url_dbus";
    public static final String GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION = "bootstrap.servers.version";
    public static final String CONF_KEY_GLOBAL_EVENT_TOPIC = "global.event.topic"; // 全局事件topic
    public static final String PROJECT_REMINDER_TIME = "project.reminder.time";//项目有效期提醒时间

    public static final String STORM_JAR_DIR = "dbus_jars";

    /**
     * zk节点
     */
    public static final String KEEPER_CTLMSG_PRODUCER_CONF = "/DBus/Keeper/ctlmsg-producer.properties";
    public static final String KEEPER_CONSUMER_CONF = "/DBus/Keeper/consumer.properties";
    public static final String KEEPER_CONFTEMPLATE_CONFIGURE = "/DBus/ConfTemplates/Topology/placeholder-appender/configure.properties";
    public static final String KEEPER_PROJECT_CONFIG = "project-config.properties";
    public static final String GLOBAL_CONF = "/DBus/Commons/global.properties";
    public static final String MGR_DB_CONF = "/DBus/Commons/mysql.properties";

    public static final String OK = "ok";
    public static final String ABORT = "abort";
    public static final String WAITING = "waiting";
    public static final String ACTIVE = "active";


    public static final String UTF8 = "utf-8";

    public static final String SPLIT_COL_TYPE_PK = "SPLIT_COL_TYPE_PK";
    public static final String SPLIT_COL_TYPE_UK = "SPLIT_COL_TYPE_UK";
    public static final String SPLIT_COL_TYPE_COMMON_INDEX = "SPLIT_COL_TYPE_COMMON_INDEX";

    /**
     * ogg 相关
     */
    public static final String OGG_PATH = "oggPath";
    public static final String OGG_TOOL_PATH = "oggToolPath";
    public static final String NLS_LANG = "NLS_LANG";
    public static final String DS_OGG_CONF = "dsOggConf";
    public static final String TRAIL_NAME = "trailName";
    public static final String REPLICAT_NAME = "replicatName";
    public static final String HOSTS = "hosts";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USER = "user";
    public static final String CANAL_PATH = "canalPath";
    public static final String CANAL_ADD = "canalAdd";
    public static final String DS_CANAL_CONF = "dsCanalConf";


    /**
     * 处理zk节点需要
     */
    public static final String DS_NAME_PLACEHOLDER = "placeholder";
    public static final String BOOTSTRAP_SERVER_PLACEHOLDER = "[BOOTSTRAP_SERVER_PLACEHOLDER]";
    public static final String ZK_SERVER_PLACEHOLDER = "[ZK_SERVER_PLACEHOLDER]";


    /**
     * router reload type
     */
    public static final String ROUTER_RELOAD = "ROUTER_RELOAD";
    public static final String ROUTER_TOPOLOGY_TABLE_EFFECT = "ROUTER_TOPOLOGY_TABLE_EFFECT";
    public static final String ROUTER_TOPOLOGY_TABLE_START = "ROUTER_TOPOLOGY_TABLE_START";
    public static final String ROUTER_TOPOLOGY_TABLE_STOP = "ROUTER_TOPOLOGY_TABLE_STOP";
    public static final String ROUTER_TOPOLOGY_EFFECT = "ROUTER_TOPOLOGY_EFFECT";
    public static final String ROUTER_TOPOLOGY_RERUN = "ROUTER_TOPOLOGY_RERUN";

}
