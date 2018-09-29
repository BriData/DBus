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

package com.creditease.dbus.common;

public class DataPullConstants {
    public static final String ZK_NODE_NAME_ORACLE_CONF = "oracle-config";
    public static final String ZK_NODE_NAME_MYSQL_CONF = "mysql-config";

    public static final String DATA_SOURCE_INFO = "data.source.param";
    public static final String DATA_SOURCE_NAME_SPACE = "dsNameSpace";
    public static final String DATA_MONITOR_ZK_PATH = "monitor.ZKPath";
    public static final String DATA_SOURCE_NAME = "dsName";
    public static final String DATA_SOURCE_TYPE = "dsType";
    public static final String DATA_SOURCE_TOPIC = "topic";
    public static final String DATA_SOURCE_MASTER_URL = "masterUrl";
    public static final String DATA_SOURCE_SLAVE_URL = "slaveUrl";
    public static final String DATA_SOURCE_USERNAME = "user";
    public static final String DATA_SOURCE_PASSWORD = "pwd";
    
    public static final String QUERY_COND_IS_NULL = " IS NULL";
    
    public static final String PULL_COLLATE_KEY = "pull.collate";
    public static final String SPLITTER_STRING_STYLE_DEFAULT = "all";

    public static final String FULL_DATA_PULL_REQ_PROJECT_ID = "id";
    public static final String FULL_DATA_PULL_REQ_PROJECT_NAME = "name";
    public static final String FULL_DATA_PULL_REQ_PROJECT_SINK_ID = "sink_id";
    public static final String FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID = "topo_table_id";

    public static final String FULL_DATA_PULL_REQ_RESULT_TOPIC= "resultTopic";
    public static final String FULL_DATA_PULL_REQ_INCREASE_VERSION= "INCREASE_VERSION";
    public static final String FULL_DATA_PULL_REQ_INCREASE_BATCH_NO= "INCREASE_BATCH_NO";
    
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_DATA_SOURCE_ID = "DBUS_DATASOURCE_ID";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME = "SCHEMA_NAME";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_POS = "POS";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME = "TABLE_NAME";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_PHYSICAL_TABLES = "PHYSICAL_TABLES";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SCN_NO ="SCN_NO";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SEQNO ="SEQNO";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_COL ="SPLIT_COL";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_SHARD_SIZE = "SPLIT_SHARD_SIZE";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_STYLE = "SPLIT_SHARD_STYLE";

    public static final String FULL_DATA_PULL_REQ_PAYLOAD_SPLIT_BOUNDING_QUERY ="SPLIT_BOUNDING_QUERY";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_INPUT_CONDITIONS ="INPUT_CONDITIONS";
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_PULL_TARGET_COLS ="PULL_TARGET_COLS";    
    public static final String FULL_DATA_PULL_REQ_PAYLOAD_OP_TS = "OP_TS";
    public static final String DATA_CHUNK_SPLIT = "dataChunkSplit";
    public static final String DATA_CHUNK_SPLIT_INDEX = "dataChunkSplitIndex";
    public static final String DATA_CHUNK_COUNT = "dataChunkCount";
    public static final String DATA_EVENT_FULL_PULL_REQ = "FULL_DATA_PULL_REQ";
    public static final String DATA_EVENT_INDEPENDENT_FULL_PULL_REQ = "FULL_DATA_INDEPENDENT_PULL_REQ";
    public static final String COMMAND_FULL_PULL_STOP = "FULL_DATA_PULL_STOP";
    public static final String COMMAND_FULL_PULL_RELOAD_CONF = "FULL_DATA_PULL_RELOAD_CONF";
    
    public static final String SPLIT_COL_TYPE_PK = "SPLIT_COL_TYPE_PK";
    public static final String SPLIT_COL_TYPE_UK = "SPLIT_COL_TYPE_UK";
    public static final String SPLIT_COL_TYPE_COMMON_INDEX = "SPLIT_COL_TYPE_COMMON_INDEX";
    /**
     * 数据源存储的key值
     */
    public static final String DATA_SOURCE = "__datasource_key__";


    public static class FullPullHistory {
        public static final String FULL_PULL_REQ_MSG_OFFSET = "full_pull_req_msg_offset";
        public static final String FIRST_SHARD_MSG_OFFSET = "first_shard_msg_offset";
        public static final String LAST_SHARD_MSG_OFFSET = "last_shard_msg_offset";

    }
    /**
     * 定义内部类用于将常量分组
     */
    public static class FullDataPullTopoItems {
        //splitting Topo
        public static final String DATA_SPLITTING_SPOUT_NAME = "fullDataSplittingSpout";
        public static final String DATA_SPLITTING_BOLT_NAME = "fullDataSplittingBolt";
        
        //batch data fetching topo
        public static final String PULLING_SPOUT_NAME = "fullDataPullingSpout";
        public static final String BATCH_DATA_FETCHING_BOLT_NAME = "batchDataFetchingBolt";
        public static final String UID_ADDING_BOLT_NAME = "uniqIdAddingBolt";
        public static final String PROGRESS_BOLT_NAME = "progressBolt";
    }
    
    public static final String DATA_PULL_RESULT_JSON = "dataPullResultJson";    
    public static final String DATA_PULL_TARGET_TOPIC = "dataPullResultTargetTopic";
    
    public static final String KAFKA_SEND_BATCH_SIZE = "send.batch.size";
    public static final String KAFKA_SEND_ROWS = "send.rows";
    public static final String STORM_UI = "storm.ui";
    
    public static class ZkMonitoringJson {
        public static final String HEART_BEAT_CONTROL_CMD_TYPE = "cmdType";
        public static final String HEART_BEAT_CONTROL_ARGS = "args";

        public static final String FULLPULLER_NODE_UPDATE_TIME = "UpdateTime";
        public static final String FULLPULLER_NODE_READ_OFFSET = "ConsumerOffset";
        public static final String FULLPULLER_NODE_WRITE_OFFSET = "ProducerOffset";
        
        public static final String DB_NAMESPACE_NODE_CLEAN_BEFORE_UPDATE = "cleanBeforeUpdate";
        public static final String DB_NAMESPACE_NODE_UPDATE_TIME = "UpdateTime";
        public static final String DB_NAMESPACE_NODE_CREATE_TIME  = "CreateTime";
        public static final String DB_NAMESPACE_NODE_START_TIME  = "StartTime";
        public static final String DB_NAMESPACE_NODE_END_TIME = "EndTime";
        public static final String DB_NAMESPACE_NODE_ERROR_MSG = "ErrorMsg";
        public static final String DB_NAMESPACE_NODE_TOTAL_COUNT = "TotalCount";
        public static final String DB_NAMESPACE_NODE_FINISHED_COUNT = "FinishedCount";
        public static final String DB_NAMESPACE_NODE_PARTITIONS = "Partitions";
        public static final String DB_NAMESPACE_NODE_TOTAL_ROWS = "TotalRows";
        public static final String DB_NAMESPACE_NODE_FINISHED_ROWS = "FinishedRows";
        public static final String DB_NAMESPACE_NODE_CONSUME_SECS = "ConsumeSecs";
        public static final String DB_NAMESPACE_NODE_START_SECS = "StartSecs";
    }
    
    public static class FullPullInterfaceJson {
        public static final String PAYLOAD_KEY = "payload";
        public static final String ID_KEY = "id";
        public static final String TIMESTAMP_KEY = "timestamp";
        public static final String FROM_KEY = "from";
        public static final String COMPLETE_TIME_KEY = "COMPLETE_TIME";
        public static final String TYPE_KEY = "type";
        public static final String DATA_STATUS_KEY = "STATUS";
        public static final String VERSION_KEY = "VERSION";
        public static final String BATCH_NO_KEY = "BATCH_NO";
        
        public static final String FROM_VALUE = "dbus-full-data-puller";
        public static final String TYPE_VALUE = "APPENDER_TOPIC_RESUME";
    }
    
    public static class TopoStopStatus {
        public static final String READY = "Ready";
        public static final String KILLED = "Killed Successfully.";
        public static final String KILLING_FAILED = "Failed to kill Topo.";
    }
    
    public static class TopoStopResult {
        public static final String STATUS_KEY = "status";
        public static final String SUCCESS = "SUCCESS";
        public static final String FAILED = "FAILED";
    }
    
    public static final String FULLPULL_PENDING_TASKS = "pendingTasks-";
    public static final String FULLPULL_PENDING_TASKS_RESOLVED = "pendingTasksResolved-";
    public static final String FULLPULL_PENDING_TASKS_OP_ADD_WATCHING = "add_to_watching_list";
    public static final String FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING = "remove_from_watching_list";
    public static final String FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY = "crashed_notify";

    public static final String FULLPULL_PENDING_TASKS_OP_PULL_CHECK = "pull_check";
    public static final String FULLPULL_PENDING_TASKS_OP_SPLIT_CHECK = "split_check";
    public static final String FULLPULL_PENDING_TASKS_OP_PULL_OR_SPLIT_EXCEPTION = "pull_or_split_Exception";
    public static final String FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART = "pull_topology_restart";
    public static final String FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART = "split_topology_restart";

    public static final int DEFAULT_SPLIT_SHARD_SIZE = 100000;
    public static final int DEFAULT_PREPARE_STATEMENT_FETCH_RECORDS = 2500; // 默认每次fetch 2500行
    private DataPullConstants () {}
}
