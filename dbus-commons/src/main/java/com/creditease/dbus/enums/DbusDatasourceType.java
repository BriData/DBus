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

package com.creditease.dbus.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Shrimp on 16/8/19.
 */
public enum DbusDatasourceType {
    ORACLE, MYSQL,
    LOG_LOGSTASH, LOG_LOGSTASH_JSON, LOG_UMS, LOG_FLUME, LOG_FILEBEAT,
    JSONLOG, ES_SQL_BATCH, MONGO, UNKNOWN;

    public static Map<String, DbusDatasourceType> map;
    public static final String ALIAS_FOR_ALL_LOG_DS_TYPE = "ALIAS_FOR_ALL_LOG_DS_TYPE";

    static {
        map = new HashMap<>();
        for (DbusDatasourceType type : values()) {
            map.put(type.name().toLowerCase(), type);
        }
    }

    public static boolean stringEqual(String type, DbusDatasourceType dbusDatasourceType) {
        return parse(type) == dbusDatasourceType;
    }

    public static DbusDatasourceType parse(String type) {
        type = type.toLowerCase();
        if (map.containsKey(type)) {
            return map.get(type);
        }
        return DbusDatasourceType.UNKNOWN;
    }

    public static String getDataBaseDriverClass(DbusDatasourceType dbusDatasourceType) {
        switch(dbusDatasourceType) {
            case    ORACLE: return "oracle.jdbc.OracleDriver";
            case    MYSQL: return "com.mysql.jdbc.Driver";
            case    LOG_LOGSTASH: return "plainlog";
            case    LOG_LOGSTASH_JSON: return "jsonlog";
            case    LOG_UMS: return "log_ums";
            case    LOG_FLUME: return "flume";
            case    ES_SQL_BATCH: return "es";
            case    MONGO: return "mongo";
            default:
                throw new RuntimeException("Wrong Database type.");
        }
    }

    public static String getAliasOfDsType(DbusDatasourceType dbusDatasourceType) {
        switch(dbusDatasourceType) {
            case    ORACLE:
                return DbusDatasourceType.ORACLE.name().toLowerCase();
            case    MYSQL:
                return DbusDatasourceType.MYSQL.name().toLowerCase();
            //各种log类型数据源，共用一种形式的zk模板。通过常量配置一个别名DS_TYPE_LOG_ALIAS，统一各种log type
            case    LOG_LOGSTASH:
            case    LOG_LOGSTASH_JSON:
            case    LOG_UMS:
            case    LOG_FLUME:
            case    LOG_FILEBEAT:
                return ALIAS_FOR_ALL_LOG_DS_TYPE.toLowerCase();
            case    ES_SQL_BATCH:
                return DbusDatasourceType.ES_SQL_BATCH.name().toLowerCase();
            case    MONGO:
                return DbusDatasourceType.MONGO.name().toLowerCase();
            default:
                throw new RuntimeException("Wrong Database type.");
        }
    }

    public static void main(String[] args) {
        System.out.println(DbusDatasourceType.LOG_LOGSTASH_JSON.name());
    }
}
