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


package com.creditease.dbus.constant;

import com.creditease.dbus.commons.Constants;

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

    /**
     * storm topology jar
     */
    public static final String CATEGORY_NORMAL = "normal";
    public static final String CATEGORY_ROUTER = "router";
    public static final String CATEGORY_SINKER = "sinker";
    public static final String TYPE_LOG_PROCESSOR = "log_processor";
    public static final String TYPE_MYSQL_EXTRACTOR = "mysql_extractor";
    public static final String TYPE_DISPATCHER_APPENDER = "dispatcher_appender";
    public static final String TYPE_SPLITTER_PULLER = "splitter_puller";
    public static final String TYPE_ROUTER = "router";
    public static final String TYPE_SINKER = "sinker";
    public static final String RELEASE_VERSION = "0.6.x";

    public static final String JAR_NAME_LOG_PROCESSOR = "dbus-log-processor-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    //public static final String JAR_NAME_MYSQL_EXTRACTOR = "dbus-mysql-extractor-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    public static final String JAR_NAME_DISPATCHER_APPENDER = "dbus-stream-main-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    public static final String JAR_NAME_SPLITTER_PULLER = "dbus-fullpuller-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    public static final String JAR_NAME_ROUTER = "dbus-router-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    public static final String JAR_NAME_SINKER = "dbus-sinker-" + Constants.RELEASE_VERSION + "-jar-with-dependencies.jar";
    public static final String JAR_NAME_ENCODER_PLUGINS = "encoder-plugins-" + Constants.RELEASE_VERSION + ".jar";


    public static final String DBUS_HEARTBEAT_ZIP = "dbus-heartbeat-" + Constants.RELEASE_VERSION + ".zip";
    public static final String DBUS_HEARTBEAT = "dbus-heartbeat-" + Constants.RELEASE_VERSION;
    public static final String DBUS_CANAL_AUTO_ZIP = "dbus-canal-auto-" + Constants.RELEASE_VERSION + ".zip";
    public static final String CANAL_ZIP = "canal.zip";
    public static final String DBUS_CANAL_AUTO = "dbus-canal-auto-" + Constants.RELEASE_VERSION;
    public static final String DBUS_OGG_AUTO_ZIP = "dbus-ogg-auto-" + Constants.RELEASE_VERSION + ".zip";
    public static final String DBUS_OGG_AUTO = "dbus-ogg-auto-" + Constants.RELEASE_VERSION;

    /**
     * zookeeper properties name
     */
    public static final String BUSSINESS_PLACEHOLDER = "placeholder";
    public static final String CONTROL_MESSAGE_SENDER_NAME = "dbus-rest";
    public static final String CONF_KEY_GLOBAL_EVENT_TOPIC = "global.event.topic"; // 全局事件topic
    public static final String PROJECT_REMINDER_TIME = "project.reminder.time";//项目有效期提醒时间

    public static final String STORM_JAR_DIR = "dbus_jars";
    public static final String STORM_ENCODER_JAR_DIR = "dbus_encoder_jars";
    public static final String STORM_KEYTAB_FILE_DIR = "dbus_keytab_path";

    /**
     * global.properties
     */
    public static final String GLOBAL_CONF_KEY_CLUSTER_SERVER_LIST = "dbus.cluster.server.list";
    public static final String GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER = "dbus.cluster.server.ssh.user";
    public static final String GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT = "dbus.cluster.server.ssh.port";
    public static final String GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION = "bootstrap.servers.version";
    public static final String GLOBAL_CONF_KEY_GRAFANA_WEB_URL = "grafana.web.url";
    public static final String GLOBAL_CONF_KEY_GRAFANA_DBUS_URL = "grafana.dbus.url";
    public static final String GLOBAL_CONF_KEY_GRAFANA_TOKEN = "grafana.token";
    public static final String GLOBAL_CONF_KEY_INFLUXDB_WEB_URL = "influxdb.web.url";
    public static final String GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL = "influxdb.dbus.url";
    public static final String GLOBAL_CONF_KEY_STORM_NIMBUS_HOST = "storm.nimbus.host";
    public static final String GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH = "storm.nimbus.home.path";
    public static final String GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH = "storm.nimbus.log.path";
    public static final String GLOBAL_CONF_KEY_STORM_REST_URL = "storm.rest.url";
    public static final String GLOBAL_CONF_KEY_STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";
    public static final String GLOBAL_CONF_KEY_ZK_STR = "zk.str";
    public static final String GLOBAL_CONF_KEY_HEARTBEAT_HOST = "heartbeat.host";
    public static final String GLOBAL_CONF_KEY_HEARTBEAT_PATH = "heartbeat.path";

    public static final String GLOBAL_CONF_KEY_JARS_PATH = "dbus.jars.path";
    public static final String GLOBAL_CONF_KEY_ENCODE_JARS_PATH = "dbus.encode.jars.path";
    public static final String GLOBAL_CONF_KEY_KEYTAB_FILE_PATH = "dbus.keytab.file.path";

    //public static final String GLOBAL_CONF_KEY_CANAL_AUTO_DIR = "dbus.canal.auto.dir.name";
    //public static final String GLOBAL_CONF_KEY_OGG_AUTO_DIR = "dbus.ogg.auto.dir.name";
    public static final String GLOBAL_CONF_KEY_KEYTAB_PATH = "keytab.path";
    public static final String GLOBAL_CONF_KEY_PRINCIPAL = "principal";

    /**
     * zk节点
     */
    public static final String KEEPER_CTLMSG_PRODUCER_CONF = "/DBus/Keeper/ctlmsg-producer.properties";
    public static final String KEEPER_CONSUMER_CONF = "/DBus/Keeper/consumer.properties";
    public static final String KEEPER_CONFTEMPLATE_CONFIGURE = "/DBus/ConfTemplates/Topology/placeholder-appender/configure.properties";
    public static final String KEEPER_PROJECT_CONFIG = "project-config.properties";
    public static final String GLOBAL_CONF = Constants.GLOBAL_PROPERTIES_ROOT;
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
    public static final String SLAVE_ID = "slaveId";
    public static final String DS_CANAL_CONF = "dsCanalConf";
    public static final String DEPLOY_CONF = "deployConf";
    public static final String MAX_CANAL_NUMBER = "maxCanalNumber";
    public static final String MAX_REPLICAT_NUMBER = "maxReplicatNumber";
    public static final String MGR_REPLICAT_PORT = "mgrReplicatPort";
    public static final String OGG_TRAIL_PATH = "oggTrailPath";


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

    public static final String FULL_PULL_PAYLOAD_SINK_TYPE = "SINK_TYPE";
    public static final String FULL_PULL_PAYLOAD_DATA_FORMAT = "DATA_FORMAT";
    public static final String FULL_PULL_PAYLOAD_HDFS_ROOT_PATH = "HDFS_ROOT_PATH";

}
