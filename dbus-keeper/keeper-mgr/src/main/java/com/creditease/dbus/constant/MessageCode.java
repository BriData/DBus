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
 * User: 王少楠
 * Date: 2018-04-20
 * Time: 下午2:18
 */
public class MessageCode {
    //异常返回
    public static final int EXCEPTION = 10000;
    //Auth model code 10xxx
    public static final int USER_NAME_EMPTY = 10001;
    public static final int PASSWORD_EMPTY = 10002;
    public static final int USER_NAME_OR_PASSWORD_INCORRECT = 10003;
    public static final int AUTHORIZATION_FAILURE  = 10004;
    public static final int AUTHORIZATION_FAILURE_PROJECT  = 10005;

    //User module code 11xxx
    public static final int MAILBOX_EMPTY = 11001;
    public static final int USER_ID_EMPTY = 11002;
    public static final int USER_NAME_OR_PWD_INCORRECT = 11003;
    public static final int OLD_PASSWORD_EMPTY = 11004;
    public static final int NEW_PASSWORD_EMPTY = 11005;

    //Sink module code 12xxx
    public static final int SINK_URL_EMPTY = 12001;
    public static final int SINK_ID_EMPTY = 12002;
    public static final int SINK_NANE_EMPTY = 12003;
    public static final int SINK_NEW_CONNECTIVITY_ERROR = 12004;

    //Topoloby module code 13xxx
    public static final int MAXIMUM_TOPOLOGY_ALLOWD = 13001;

    //Project Table module code 14xxx
    public static final int PROJECT_ID_OR_TABLE_ID_EMPTY = 14001;
    public static final int SINK_URL_ERROR = 14002;
    public static final int PROJECT_ID_EMPTY = 14003;
    public static final int TABLE_ADD_LACK_MSG = 14004;
    public static final int INITIAL_LOAD_ERROR = 14005;
    public static final int TABLE_NOT_FOUND = 14006;
    public static final int TABLE_PARAM_FORMAT_ERROR = 14007;
    public static final int TABLE_RESOURCE_FULL_PULL_FALSE = 14008;
    public static final int TABLE_OUTPUT_TOPIC_ERROR = 14009;
    public static final int PLEASE_TRY_AGAIN_LATER = 14010;
    public static final int CREATE_DASHBOARD_ERROR = 14011;

    //Table module code 15xxx
    public static final int TABLE_NOT_FOUND_BY_ID = 15001;
    public static final int TABLE_VERSION_NOT_FOUND_BY_ID = 15002;
    public static final int TABLE_IS_WAITING_FOR_LOADING_DATA = 15003;
    public static final int TABLE_DATA_LOADING_IS_EXPIRED = 15004;
    public static final int INITIAL_LOAD_IS_RUNNING = 15005;
    public static final int TABLE_IS_WAITING_FOR_INITIAL_LOAD = 15006;
    public static final int TABLE_ID_EMPTY = 15007;
    public static final int VERSIONID_EMPTY = 15008;
    public static final int GROUP_ID_EMPTY = 15009;
    public static final int TABLE_NOT_FOUND_BY_PARAM = 15010;
    public static final int TYPE_OF_TABLE_CAN_NOT_FULLPULL = 15011;
    public static final int TABLE_ALREADY_BE_USING = 15012;


    //Datasource module code 16xxx
    public static final int DATASOURCE_KILL_TOPO_NONE_TOPO_ID = 16001;
    public static final int DATASOURCE_KILL_TOPO_FAILED = 16002;
    public static final int DATASOURCE_KILL_TOPO_EXCEPTION = 16003;
    public static final int DATASOURCE_START_TOPO_FAILED = 16004;
    public static final int DATASOURCE_ALREADY_BE_USING = 16005;
    public static final int DATASOURCE_TYPE_UNKNOWN = 16006;

    //ConfigCenter module code 17xxx
    public static final int DBUS_MGR_DB_FAIL_WHEN_CONNECT = 17001;
    public static final int INIT_ZOOKEEPER_ERROR = 17002;
    public static final int HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR = 17004;
    public static final int INFLUXDB_URL_ERROR = 17005;
    public static final int KAFKA_BOOTSTRAP_SERVERS_IS_WRONG = 17006;
    public static final int MONITOR_URL_IS_WRONG = 17007;
    public static final int GRAFANATOKEN_IS_ERROR = 17008;
    public static final int STORM_SSH_SECRET_CONFIGURATION_ERROR = 17009;
    public static final int DBUS_MGR_INIT_ERROR = 17010;
    public static final int CREATE_DEFAULT_SINK_ERROR = 17011;
    public static final int CREATE_SUPER_USER_ERROR = 17012;
    public static final int ENCODE_PLUGIN_INIT_ERROR = 17013;
    public static final int STORM_HOME_PATH_ERROR = 17014;
    public static final int HEARTBEAT_JAR_PATH_ERROR = 17015;
    public static final int STORM_UI_ERROR = 17016;
    public static final int DBUS_ENVIRONMENT_IS_ALREADY_INIT = 17017;

    //ToolSet module code 18xxx
    public static final int PARAM_IS_WRONG = 18001;
    public static final int MESSAGE_IS_WRONG = 18002;
    public static final int EXCEPTION_ON_SEND_MESSAGE = 18003;
    public static final int LINE_NUMBER_IS_WRONG = 18004;
    public static final int NO_AUTHORITY_FOR_THIS_TOPIC = 18005;
    public static final int FULLPULL_TOPO_IS_NOT_RUNNING = 18006;
    public static final int TOPO_TYPE_AND_DSTYPE_NOT_MATCH = 18007;
    public static final int UNSUPPORTED_CTRL_MESSAGE_TYPES = 18008;

    //Project resource module
    public static final int PROJECT_RESOURCE_IS_USING = 19001;

    //schema module
    public static final int SCHEMA_IS_USING = 20001;

    //Project module
    public static final int PROJECT_INACTIVE = 21001;
    public static final int PROJECT_NOT_ALLOWED_DELETED = 21002;

    //ogg canal module
    public static final int STOP_REPLICAT_ERROR = 22001;
    public static final int START_REPLICAT_ERROR = 22002;
    public static final int DEPLOY_REPLICAT_ERROR = 22003;
    public static final int ADD_REPLICAT_ERROR = 22004;
    public static final int SSH_CONF_ERROR = 22005;
    public static final int DEPLOY_CANAL_ERROR = 22006;

    //fullpull
    public static final int ILLEGAL_CONTROL_MESSAGE = 23001;
    public static final int EXCEPTION_WHEN_GOLBAL_FULLPULL = 23002;

    // project topo module code 3xxxx
    public static final int ACHIEVE_TOPOLOGY_MAX_COUNT = 30001;
    public static final int PROJECT_NOT_EXIST = 30002;
    public static final int TOPOLOGY_NOT_EXIST = 30003;
    public static final int TOPOLOGY_RUNNING_DO_NOT_DELETE = 30004;
    public static final int TOPOLOGY_NAME_EXIST = 30005;

    // dba encorde module code 4xxxx
    public static final int DBA_ENCODE_FETCH_DBINFO = 40001;
    public static final int DBUS_DB_INFO_IS_EMPTY = 40002;
    public static final int CREATE_DBA_ENCODE_BACK_TABLE_FIAL = 40003;
    public static final int BACKUP_DBA_ENCODE_DATA_FIAL = 40004;
    public static final int SYNCHRONOUS_ENCODE_DATA_ALL_SUCCESS = 40005;
    public static final int SYNCHRONOUS_ENCODE_DATA_PART_SUCCESS = 40006;
    public static final int SYNCHRONOUS_ENCODE_DATA_ALL_FAIL = 40007;
    public static final int FETCH_ALL_TABLE_IDS = 40008;
    public static final int TABLE_IDS_IS_EMPTY = 40009;
    public static final int CONVERT_AND_EFFECT_ALL_SUCCESS = 40010;
    public static final int CONVERT_AND_EFFECT_PART_SUCCESS = 40011;
    public static final int CONVERT_AND_EFFECT_ALL_FAIL = 40012;
    public static final int SESSION_IS_NOT_EXISTS = 40013;
    public static final int ENCODE_PLUGINS_NOT_EXISTS = 40014;

}
