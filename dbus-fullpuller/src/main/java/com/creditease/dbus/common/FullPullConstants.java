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


package com.creditease.dbus.common;

import com.creditease.dbus.commons.Constants;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/11/17
 */
public class FullPullConstants {

    /**
     * 全量状态
     */
    public static final String FULL_PULL_STATUS_SPLITTING = "splitting";
    public static final String FULL_PULL_STATUS_PULLING = "pulling";
    public static final String FULL_PULL_STATUS_ENDING = "ending";
    public static final String FULL_PULL_STATUS_ABORT = "abort";

    /**
     * 全量spout类型
     */
    public static final String FULL_SPLITTER_TYPE = "full-splitter";
    public static final String FULL_PULLER_TYPE = "full-puller";

    /**
     * stream name
     */
    public static final String CTRL_STREAM = "ctrlStream";
    /**
     * 数据库连接属性
     */
    public static final String DB_CONF_PROP_KEY_URL = "url";
    public static final String DB_CONF_PROP_KEY_USERNAME = "username";
    public static final String DB_CONF_PROP_KEY_PASSWORD = "password";

    /**
     * DataSource param
     */
    public static final String FULLPULL_REQ_PARAM = "fullPull.request.param";
    public static final String DATA_SOURCE_NAME_SPACE = "dsNameSpace";
    public static final String DATA_MONITOR_ZK_PATH = "monitor.ZKPath";
    public static final String DATA_SOURCE_NAME = "dsName";
    public static final String DATA_SOURCE_TYPE = "dsType";
    public static final String DATA_SOURCE_TOPIC = "topic";
    public static final String DATA_SOURCE_MASTER_URL = "masterUrl";
    public static final String DATA_SOURCE_SLAVE_URL = "slaveUrl";
    public static final String DATA_SOURCE_USERNAME = "user";
    public static final String DATA_SOURCE_PASSWORD = "pwd";

    /**
     * zk节点
     */
    public static final String DBUS_ROOT = "/DBus";
    public static final String ENCODER_PLUGINS = "/DBus/Commons/encoderPlugins";

    /**
     * 一级目录
     */
    public static final String COMMON_ROOT = DBUS_ROOT + "/Commons";

    public static final String HEARTBEAT_CONTROL_NODE = Constants.HEARTBEAT_ROOT + "/Control";
    public static final String FULL_PULL_MONITOR_ROOT_GLOBAL = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
    public static final String FULL_PULL_MONITOR_ROOT = Constants.FULL_PULL_MONITOR_ROOT;
    public static final String FULL_PULL_PROJECTS_MONITOR_ROOT = Constants.FULL_PULL_PROJECTS_MONITOR_ROOT;
    public static final String TOPOLOGY_ROOT = Constants.TOPOLOGY_ROOT;
    public static final String FULL_SPLITTING_PROPS_ROOT = Constants.FULL_SPLITTING_PROPS_ROOT;
    public static final String FULL_PULLING_PROPS_ROOT = Constants.FULL_PULLING_PROPS_ROOT;

    /**
     * zk节点名称
     */
    public static final String ZK_NODE_NAME_ORACLE_CONF = "oracle-config";
    public static final String ZK_NODE_NAME_MYSQL_CONF = "mysql-config";
    public static final String ZK_NODE_NAME_MONGO_CONF = "mongo-config";
    public static final String ZK_NODE_NAME_DB2_CONF = "db2-config";
    public static final String COMMON_CONFIG = "common-config";
    public static final String MYSQL_CONFIG = "mysql";
    public static final String CONSUMER_CONFIG = "consumer-config";
    public static final String HDFS_CONFIG = "hdfs-config";

    /**
     * zk节点属性名称
     */
    public static final String GLOBAL_FULLPULLER_TOPO_PREFIX = "global-";
    public static final String BYTE_PRODUCER_CONFIG = "byte-producer-config";
    public static final String STRING_PRODUCER_CONFIG = "string-producer-config";

    public static final String OUTPUT_VERSION = "output.version";
    public static final String FULL_PULL_MONITORING_ZK_CONN_STR = "monitor.zk";

    public static final String FULL_PULL_SRC_TOPIC = "fullpull.src.topic";
    public static final String FULL_PULL_MEDIANT_TOPIC = "fullpull.mediant.topic";
    public static final String FULL_PULL_CALLBACK_TOPIC = "fullpull.callback.topic";
    public static final String SPLITTING_BOLT_PARALLEL = "splitting.bolt.parallel";
    public static final String PULLING_BOLT_PARALLEL = "pulling.bolt.parallel";
    public static final String HEARTBEAT_MONITOR_TIME_INTERVAL = "heartbeat.monitor.time.interval";
    public static final String STORM_NUM_WORKERS = "storm.num.workers";
    public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
    public static final String STORM_MESSAGE_TIMEOUT = "storm.message.timeout";
    public static final String STORM_MAX_SPOUT_PENDING = "storm.max.spout.pending";

    public static final String HDFS_URL = "hdfs.url";
    public static final String CORE_SITE = "core.site";
    public static final String HDFS_SITE = "hdfs.site";
    public static final String HADOOP_USER_NAME = "hadoop.user.name";

    /**
     * ZkMonitoringJson key
     */
    public static final String HEART_BEAT_CONTROL_CMD_TYPE = "cmdType";
    public static final String HEART_BEAT_CONTROL_ARGS = "args";

    public static final String FULLPULLER_NODE_UPDATE_TIME = "UpdateTime";
    public static final String FULLPULLER_NODE_READ_OFFSET = "ConsumerOffset";
    public static final String FULLPULLER_NODE_WRITE_OFFSET = "ProducerOffset";

    public static final String DB_NAMESPACE_NODE_CLEAN_BEFORE_UPDATE = "cleanBeforeUpdate";
    public static final String DB_NAMESPACE_NODE_UPDATE_TIME = "UpdateTime";
    public static final String DB_NAMESPACE_NODE_CREATE_TIME = "CreateTime";
    public static final String DB_NAMESPACE_NODE_START_TIME = "StartTime";
    public static final String DB_NAMESPACE_NODE_END_TIME = "EndTime";
    public static final String DB_NAMESPACE_NODE_ERROR_MSG = "ErrorMsg";
    public static final String DB_NAMESPACE_NODE_TOTAL_COUNT = "TotalCount";
    public static final String DB_NAMESPACE_NODE_FINISHED_COUNT = "FinishedCount";
    public static final String DB_NAMESPACE_NODE_PARTITIONS = "Partitions";
    public static final String DB_NAMESPACE_NODE_TOTAL_ROWS = "TotalRows";
    public static final String DB_NAMESPACE_NODE_FINISHED_ROWS = "FinishedRows";
    public static final String DB_NAMESPACE_NODE_CONSUME_SECS = "ConsumeSecs";
    public static final String DB_NAMESPACE_NODE_START_SECS = "StartSecs";


    /**
     * 默认值
     */
    public static final long HEARTBEAT_MONITOR_TIME_INTERVAL_DEFAULT_VAL = 60L; //单位：秒 原来设置值为180（3min）;
    public static final int DEFAULT_SPLIT_SHARD_SIZE = 100000;
    public static final int DEFAULT_PREPARE_STATEMENT_FETCH_RECORDS = 2500; // 默认每次fetch 2500行
    public static final String PULL_COLLATE_KEY = "pull.collate";
    public static final String SPLITTER_STRING_STYLE_DEFAULT = "all";
    public static final String QUERY_COND_IS_NULL = " IS NULL";

    /**
     * fullpull request json key
     */
    public static final String REQ_FROM = "from";
    public static final String REQ_PAYLOAD = "payload";
    public static final String REQ_PROJECT = "project";
    public static final String REQ_TIMESTAMP = "timestamp";
    public static final String REQ_TYPE = "type";

    public static final String REQ_PAYLOAD_DATA_SOURCE_ID = "DBUS_DATASOURCE_ID";
    public static final String REQ_PAYLOAD_SCHEMA_NAME = "SCHEMA_NAME";
    public static final String REQ_PAYLOAD_TABLE_NAME = "TABLE_NAME";
    public static final String REQ_PAYLOAD_PHYSICAL_TABLES = "PHYSICAL_TABLES";
    public static final String REQ_PAYLOAD_RESULTTOPIC = "resultTopic";
    public static final String REQ_PAYLOAD_INCREASE_VERSION = "INCREASE_VERSION";
    public static final String REQ_PAYLOAD_INCREASE_BATCH_NO = "INCREASE_BATCH_NO";
    public static final String REQ_PAYLOAD_SPLIT_BOUNDING_QUERY = "SPLIT_BOUNDING_QUERY";
    public static final String REQ_PAYLOAD_SPLIT_SHARD_STYLE = "SPLIT_SHARD_STYLE";
    public static final String REQ_PAYLOAD_OP_TS = "OP_TS";
    public static final String REQ_PAYLOAD_PULL_REMARK = "PULL_REMARK";
    public static final String REQ_PAYLOAD_OGG_OP_TS = "OGG_OP_TS";
    public static final String REQ_PAYLOAD_PULL_TARGET_COLS = "PULL_TARGET_COLS";
    public static final String REQ_PAYLOAD_SEQNO = "SEQNO";
    public static final String REQ_PAYLOAD_SCN_NO = "SCN_NO";
    public static final String REQ_PAYLOAD_POS = "POS";
    public static final String REQ_PAYLOAD_SPLIT_SHARD_SIZE = "SPLIT_SHARD_SIZE";
    public static final String REQ_PAYLOAD_SPLIT_COL = "SPLIT_COL";
    public static final String REQ_PAYLOAD_VERSION = "VERSION";
    public static final String REQ_PAYLOAD_BATCH_NO = "BATCH_NO";
    public static final String REQ_PAYLOAD_INPUT_CONDITIONS = "INPUT_CONDITIONS";
    public static final String REQ_PAYLOAD_COMPLETE_TIME = "COMPLETE_TIME";
    public static final String REQ_PAYLOAD_STATUS = "STATUS";
    public static final String REQ_PAYLOAD_SINK_TYPE = "SINK_TYPE";
    public static final String REQ_PAYLOAD_DATA_FORMAT = "DATA_FORMAT";
    public static final String REQ_PAYLOAD_HDFS_ROOT_PATH = "HDFS_ROOT_PATH";

    public static final String REQ_PROJECT_TOPO_TABLE_ID = "topo_table_id";
    public static final String REQ_PROJECT_NAME = "name";
    public static final String REQ_PROJECT_SINK_ID = "sink_id";
    public static final String REQ_PROJECT_ID = "id";
    public static final String REQ_PROJECT_TOPO_NAME = "topo_name";

    /**
     * fullpullhistory key
     */
    public static final String FULL_PULL_REQ_MSG_OFFSET = "full_pull_req_msg_offset";


    public static final String TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER = ";";
    public static final String TABLE_SPLITTED_PHYSICAL_TABLES_KEY = "PHYSICAL_TABLES";
    public static final String SEND_BATCH_SIZE = "send.batch.size";
    public static final String SEND_ROWS = "send.rows";
    public static final String HDFS_FILE_SIZE = "hdfs.file.size";

    /**
     * StormConfigKey
     */
    public static final String FULL_SPLITTER_TOPOLOGY_ID = "full_splitter_topology_id";
    public static final String FULL_PULLER_TOPOLOGY_ID = "full_puller_topology_id";
    public static final String DS_NAME = "ds_name";
    public static final String ZKCONNECT = "zkconnect";

    public static final String FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART = "pull_topology_restart";
    public static final String FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART = "split_topology_restart";


    /**
     *
     */
    public static final String DATA_CHUNK_SPLIT = "dataChunkSplit";
    public static final String DATA_CHUNK_SPLIT_INDEX = "dataChunkSplitIndex";

    /**
     * ctrlTopic key类型
     */
    public static final String COMMAND_FULL_PULL_RELOAD_CONF = "FULL_DATA_PULL_RELOAD_CONF";
    public static final String COMMAND_FULL_PULL_FINISH_REQ = "FULL_DATA_FINISH_REQ";
    //0.6.0以后不再支持阻塞式全量
    //public static final String DATA_EVENT_FULL_PULL_REQ = "FULL_DATA_PULL_REQ";
    public static final String DATA_EVENT_INDEPENDENT_FULL_PULL_REQ = "FULL_DATA_INDEPENDENT_PULL_REQ";

    /**
     * split column type
     */
    public static final String SPLIT_COL_TYPE_PK = "SPLIT_COL_TYPE_PK";
    public static final String SPLIT_COL_TYPE_UK = "SPLIT_COL_TYPE_UK";
    public static final String SPLIT_COL_TYPE_COMMON_INDEX = "SPLIT_COL_TYPE_COMMON_INDEX";

    public static final String SINK_TYPE_KAFKA = "KAFKA";
    public static final String SINK_TYPE_HDFS = "HDFS";
    public static final String DATA_FORMAT_UMS = "UMS";
    public static final String DATA_FORMAT_CSV = "CSV";

}
