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

package com.creditease.dbus.commons;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Constants {
    public static final String VERSION_12 = "1.2";
    public static final String VERSION_13 = "1.3";
    public static final String VERSION_14 = "1.4"; //used for mongo

    public static final String SYS_PROPS_LOG4J_CONFIG = "log4j.configuration";
    public static final String SYS_PROPS_LOG4J2_CONFIG = "log4j.configurationFile";
    public static final String SYS_PROPS_LOG_BASE_PATH = "logs.base.path";
    public static final String SYS_PROPS_LOG_DIR = "log.dir";
    public static final String CONFIG_DB_KEY = "dbus.conf";

    public static final String ZOOKEEPER_SERVERS = "dbus.zookeeper.servers";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "dbus.kafka.BootstrapServers";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "dbus.zookeeper.session.timeout";
    public static final String MANGER_DB_CONNECT_URL = "dbus.manger.DB.connect.url";
    public static final String TOPOLOGY_ID = "dbus.storm.topology.dispatcher.id";
    public static final String ROUTER_PROJECT_NAME = "dbus.storm.topology.router.project.name";

    public static final String STATISTIC_TOPIC = "dbus.statistic.topic";

    public static final String DBUS_DATASOURCE_TYPE = "dbus.datasource.type";

    public static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";

    //顶级目录
    public static final String DBUS_ROOT = "/DBus";
    public static final String ENCODER_PLUGINS = "/DBus/Commons/encoderPlugins";

    //一级目录
    public static final String COMMON_ROOT = DBUS_ROOT + "/Commons";
    public static final String MYSQL_PROPERTIES = "mysql.properties";
    public static final String MYSQL_PROPERTIES_ROOT = COMMON_ROOT + "/mysql.properties";
    public static final String GLOBAL_PROPERTIES = "global.properties";
    public static final String GLOBAL_PROPERTIES_ROOT = COMMON_ROOT + "/global.properties";
    public static final String OGG_PROPERTIES = "auto-deploy-ogg.conf";
    public static final String CANAL_PROPERTIES = "auto-deploy-canal.conf";
    public static final String OGG_PROPERTIES_ROOT = COMMON_ROOT + "/" + OGG_PROPERTIES;
    public static final String CANAL_PROPERTIES_ROOT = COMMON_ROOT + "/" + CANAL_PROPERTIES;

    public static final String TEMPLATE_NODE_NAME = "/ConfTemplates";
    public static final String DBUS_CONF_TEMPLATE_ROOT = DBUS_ROOT + "/ConfTemplates";

    public static final String CONTROL_MESSAGE_RESULT_ROOT = DBUS_ROOT + "/ControlMessageResult";

    public static final String EXTRACTOR_ROOT = DBUS_ROOT + "/Extractor";

    public static final String FULL_PULL_MONITOR_ROOT = DBUS_ROOT + "/FullPuller";
    public static final String FULL_PULL_PROJECTS_MONITOR_ROOT = DBUS_ROOT + "/FullPuller/Projects";
    public static final String FULL_PULL_MONITOR_ROOT_GLOBAL = DBUS_ROOT + "/FullPullerGlobal";

    public static final String HEARTBEAT_ROOT = DBUS_ROOT + "/HeartBeat";
    public static final String HEARTBEAT_CONFIG = HEARTBEAT_ROOT + "/Config";
    public static final String HEARTBEAT_PROJECT_MONITOR = HEARTBEAT_ROOT + "/ProjectMonitor";
    public static final String HEARTBEAT_JSON = "heartbeat_config.json";
    public static final String HEARTBEAT_LEADER = HEARTBEAT_ROOT + "/Leader";
    public static final String HEARTBEAT_CONFIG_JSON = HEARTBEAT_CONFIG + "/heartbeat_config.json";

    public static final String NAMESPACE_ROOT = DBUS_ROOT + "/NameSpace";

    public static final String TOPOLOGY_ROOT = DBUS_ROOT + "/Topology";

    public static final String ROUTER_ROOT = DBUS_ROOT + "/Router";

    public static final String STREAMING_ROOT = DBUS_ROOT + "/Streaming";

    /**
     * zookeeper kafka节点path
     */
    public static final String KAFKA_ROOT = "/kafka";
    public static final String BROKERS_ROOT = KAFKA_ROOT + "/brokers";
    public static final String BROKERS_IDS = BROKERS_ROOT + "/ids";

    public static final String RULE_TYPE_STRING = "string";

    public static final String RULE_TYPE_REGEX = "regex";

    public static final String RULE_TYPE_INDEX = "index";

    public static final String PREFIX_OR_APPEND_BEFORE = "before";

    public static final String PREFIX_OR_APPEND_AFTER = "after";

    public static final String TRIM_LEFT = "left";
    public static final String TRIM_RIGHT = "right";
    public static final String TRIM_BOTH = "both";
    public static final String TRIM_ALL = "all";

    public static final String LOG_PROCESSOR = "log-processor";
    public static final String ROUTER = "router";
    //full puller
    public static final String HEARTBEAT_CONTROL_NODE = HEARTBEAT_ROOT + "/Control";
    public static final String FULL_SPLITTING_PROPS_ROOT = "dbus-fulldata-splitter";
    public static final String FULL_PULLING_PROPS_ROOT = "dbus-fulldata-puller";
    public static final String FULL_SPLITTER_TYPE = "full-splitter";
    public static final String FULL_PULLER_TYPE = "full-puller";
    //full puller mode
    public static final String FULL_PULLER_MODE = "/DBus/Keeper/safeMode/full_puller_mode.properties";

    public static final String CANAL_ROOT = DBUS_ROOT + "/Canal";

    public static final String KEEPER_ROOT = DBUS_ROOT + "/Keeper";

    public static final String DB_CONF_PROP_KEY_URL = "url";
    public static final String DB_CONF_PROP_KEY_USERNAME = "username";
    public static final String DB_CONF_PROP_KEY_PASSWORD = "password";

    public static final String FULL_PULL_TABLE = "DB_FULL_PULL_REQUESTS";
    public static final String META_SYNC_EVENT_TABLE = "META_SYNC_EVENT";
    public static final String HEARTBEAT_MONITOR_TABLE = "DB_HEARTBEAT_MONITOR";

    //dispatcher constants
    public static final String DISPATCHER_DATA_TOPIC = "dbus.dispatcher.topic.data";
    public static final String DISPATCHER_CTRL_TOPIC = "dbus.dispatcher.topic.ctrl";
    public static final String DISPATCHER_DBUS_SCHEMA = "dbus.dispatcher.schema";
    public static final String DISPATCHER_DBSOURCE_NAME = "dbus.dispatcher.datasource.name";
    public static final String DISPATCHER_DBSOURCE_TYPE = "dbus.dispatcher.datasource.type";
    public static final String DISPATCHER_OFFSET = "dbus.dispatcher.offset";

    public static final String DISPATCHER_CUNSUMER_PROPERTIES = "dispatcher.consumer.properties";
    public static final String DISPATCHER_PRODUCER_PROPERTIES = "dispatcher.producer.properties";
    public static final String DISPATCHER_SCHEMA_TOPICS_PROPERTIES = "dispatcher.schema.topics.properties";
    public static final String DISPATCHER_RAW_TOPICS_PROPERTIES = "dispatcher.raw.topics.properties";
    public static final String DISPATCHER_CONFIG_PROPERTIES = "dispatcher.configure.properties";

    //extractor constants
    public static final int NEED_ACK_CANAL = 1;
    public static final int NEED_ROLLBACK_CANAL = 2;
    public static final int SEND_NOT_COMPLETED = 3;
    public static final String EXTRACTOR_TOPOLOGY_ID = "extractor.topology.id";




    /**
     * org.apache.storm.Config.java 中存储的数据的key值
     */
    public static class StormConfigKey {
        public static final String FULL_SPLITTER_TOPOLOGY_ID = "full_splitter_topology_id";
        public static final String FULL_PULLER_TOPOLOGY_ID = "full_puller_topology_id";
        //public static final String TOPOLOGY_ID = "topology_id";
        //public static final String DATASOURCE = "datasource";
        public static final String ZKCONNECT = "zkconnect";
    }

    public static class InfluxDB {
        public static final String DB_URL = "influxdb.url";
        public static final String DB_NAME = "influxdb.dbname";
        public static final String TABLE_NAME = "influxdb.tablename";
    }


    /**
     * json消息中的key值信息
     */
    public static class MessageBodyKey {
        public static final String TABLE = "table";
        public static final String OP_TYPE = "op_type";
        public static final String OP_TS = "op_ts";
        public static final String NAMESPACE = "namespace";
        public static final String TABLE_NAME = "name";
        public static final String POS = "pos";
        public static final String BEFORE = "before";
        public static final String AFTER = "after";

        public static final String OP_TYPE_INSERT = "I";
        public static final String OP_TYPE_UPDATE = "U";
        public static final String OP_TYPE_DELETE = "D";
        public static final String IS_MISSING_SUFFIX = "_isMissing";

        /**
         * GenericRecord中不关心顺序的字段
         */
        public static Set<String> noorderKeys;

        static {
            Set<String> set = new HashSet<>();
            set.add(TABLE);
            set.add(OP_TYPE);
            set.add(OP_TS);
            set.add("current_ts");
            set.add(POS);
            set.add("primary_keys");
            set.add("tokens");
            noorderKeys = Collections.unmodifiableSet(set);
        }
    }

    private Constants() {
    }

    /**
     * t_data_tables 表 status 字段常量值
     */
    public static class DataTableStatus {
        /**
         * 可以正常接收数据状态
         */
        public static final String DATA_STATUS_OK = "ok";

        /**
         * 需要抛弃该表的数据状态
         */
        public static final String DATA_STATUS_ABORT = "abort";

        /**
         * 正在等待拉全量状态
         */
        public static final String DATA_STATUS_WAITING = "waiting";
    }


    public static class ZkTopoConfForFullPull {
        public static final String GLOBAL_FULLPULLER_TOPO_PREFIX = "global-";
        // config keys
        public static final String CONSUMER_CONFIG = "consumer-config";
        public static final String BYTE_PRODUCER_CONFIG = "byte-producer-config";
        public static final String STRING_PRODUCER_CONFIG = "string-producer-config";
        public static final String COMMON_CONFIG = "common-config";
        public static final String MYSQL_CONFIG = "mysql";

        public static final String DATASOURCE_NAME = "datasource.name";
        public static final String OUTPUT_VERSION = "output.version";
        public static final String ID_GENERATOR_ZK = "id.generator.zk";
        public static final String FULL_PULL_MONITORING_ZK_CONN_STR = "monitor.zk";
        public static final String SPOUT_MAX_FLOW_THRESHOLD = "spout.max.flow.threshold";

        public static final String FULL_PULL_SRC_TOPIC = "fullpull.src.topic";
        public static final String FULL_PULL_MEDIANT_TOPIC = "fullpull.mediant.topic";
        public static final String SPLITTING_BOLT_PARALLEL = "splitting.bolt.parallel";
        public static final String PULLING_BOLT_PARALLEL = "pulling.bolt.parallel";
        public static final String HEARTBEAT_MONITOR_TIME_INTERVAL = "heartbeat.monitor.time.interval";

        public static final String TOPOS_KILL_WAIT_TIME_FOR_RETRIES = "fullpull.topos.kill.waittime.for.retries";
        public static final String TOPOS_KILL_WAIT_TIMEOUT = "fullpull.topos.kill.timeout";
        public static final String TOPOS_KILL_STORM_API_WAITTIME_PARAM = "topo.kill.storm.api.waittime.param";

        public static final String MYSQL_DRIVER_NAME = "jdbc.driver.name";
        public static final String MYSQL_URL = "db.url";
        public static final String MYSQL_USER = "user";
        public static final String MYSQL_PASSWORD = "password";

        // config default values
        public static final long HEARTBEAT_MONITOR_TIME_INTERVAL_DEFAULT_VAL = 60L; //单位：秒 原来设置值为180（3min）;

        public static final long TOPOS_KILL_WAIT_TIME_FOR_RETRIES_DEFAULT_VAL = 180L;
        public static final long TOPOS_KILL_WAIT_TIMEOUT_DEFAULT_VAL = 1200L;
        public static final int TOPOS_KILL_STORM_API_WAITTIME_PARAM_DEFAULT_VAL = 15;
    }

    public static final String TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER = ";";
    public static final String TABLE_SPLITTED_PHYSICAL_TABLES_KEY = "PHYSICAL_TABLES";
    public static final String TABLE_SPLITTED_TOTAL_ROWS_KEY = "AllPhysicalTablesTotalRows";
    //    public static final String TABLE_SPLITTED_SHARDS_COUNT_KEY = "AllPhysicalTablesShardsCount";
    public static final String TABLE_SPLITTED_SHARD_SPLITS_KEY = "AllPhysicalTablesSplits";

    public static final String FULL_PULL_STATUS_SPLITTING = "splitting";
    public static final String FULL_PULL_STATUS_PULLING = "pulling";
    public static final String FULL_PULL_STATUS_ENDING = "ending";
    public static final String FULL_PULL_STATUS_UNKONWN = "unknown";
}
