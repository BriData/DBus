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

package com.creditease.dbus.stream.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Shrimp on 16/5/18.
 */
public class Constants {

    /**
     * 定义内部类用于将常量分组
     * 定义配置文件名常量,不用添加.properties后缀
     */
    public static class Properties {
        public static final String CONSUMER_CONFIG = "consumer-config";
        public static final String CONFIGURE = "configure";
        public static final String MYSQL = "mysql";
        public static final String ORA_META = "ora-meta";
        public static final String PRODUCER_CONFIG = "producer-config";
        public static final String PRODUCER_CONTROL = "producer-control";
    }

    /**
     * 配置文件的中的key值, Properties.CONFIGURE 配置文件中程序中使用到的key值常量
     */
    public static class ConfigureKey {
        public static final String DATASOURCE_NAME = "datasource.name";
        public static final String AVAILABLE_SCHEMAS = "available.schemas";
        public static final String FULLDATA_REQUEST_SRC = "fulldata.request.src"; // 拉全量请求表名
        public static final String HEARTBEAT_SRC = "heartbeat.src"; // 心跳表名
        public static final String META_EVENT_SRC = "meta.event.src"; // meta同步表名
        public static final String SPOUT_MAX_FLOW_THRESHOLD = "spout.max.flow.threshold";
        public static final String UMS_PAYLOAD_MAX_COUNT = "ums.payload.max.count"; // ums中payload记录数的最大值
        public static final String UMS_PAYLOAD_MAX_SIZE = "ums.payload.max.size"; // ums中payload大小最大值,单位Byte
        public static final String DBUS_STATISTIC_TOPIC = "dbus.statistic.topic"; // 统计信息topic
        public static final String GLOBAL_EVENT_TOPIC = "global.event.topic"; // 全局事件topic
        public static final String LOGFILE_NUM_COMPENSATION = "logfile.num.compensation"; // mysql binlog 文件号补偿值
        public static final String META_FETCHER_BOLT_PARALLELISM = "metafetcher.bolt.parallelism";
        public static final String KAFKA_WRITTER_BOLT_PARALLELISM = "kafkawritter.bolt.parallelism";
        public static final String WRAPPER_BOLT_PARALLELISM = "wrapper.bolt.parallelism";
        public static final String MAX_SPOUT_PENDING = "max.spout.pending";
        public static final String BASE64_DECODE = "base64.decode"; // 是否需要使用base64解码
        public static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url"; 
        public static final String DATASOURCE_TYPE = "dbus.dispatcher.datasource.type";

        /**
         * oracle for bigdata 12.3.1.1.1版本解决了函数索引的问题，在生成avro schema时需要过滤掉虚拟列和隐藏列
         * 在12.2版本中需要配置为: 0 ，在12.3版本中配置为: 1
         */
        public static final String IGNORE_VIRTUAL_FIELDS = "ignore.virtual.fields";

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





    public static class UmsMessage {
        public static final String BEFORE_UPDATE_OPERATION = "b";
        public static final String NAMESPACE_INDISTINCTIVE_SCHEMA = "schema";
        public static final String NAMESPACE_INDISTINCTIVE_TABLE = "table";
        public static final String NAMESPACE_INDISTINCTIVE_VERSION = "version";
    }

    /**
     * Cache 名字常量
     */
    public static class CacheNames {
        /**  Avro Schema保存的本地缓存名称 */
        public static final String AVRO_SCHEMA_CACHE = "avro_schema_cache";
        /** 存储meta当前版本的cache */
        public static final String META_VERSION_CACHE = "meta_version_cache";
        /** 数据表缓存 */
        public static final java.lang.String DATA_TABLES = "dbus_data_table_cache";
        /** table schema 缓存 */
        public static final String TAB_SCHEMA = "dbus_table_schema_cache";
        /** 需要脱敏的列缓存 */
        public static final String TAB_ENCODE_FIELDS = "tab_encode_fields";
        /** 脱敏插件缓存 */
        public static final String TAB_CENCODE_PLUGINS = "tab_encode_plugins";
        /** 输出版本号缓存 */
        public static final String OUTPUT_VERSION_CACHE = "output_meta_version_cache";

        public static final String TABLE_SCHEMA_VERSION_CACHE = "table_schema_version_cache";

        public static final String TABLE_CURRENT_SCHEMA_CACHE = "table_current_schema_cache";
    }

    /**
     * org.apache.storm.Config.java 中存储的数据的key值
     */
    public static class StormConfigKey {
        public static final String TOPOLOGY_ID = "topology_appender_id";
        public static final String DATASOURCE = "datasource";
        public static final String ZKCONNECT = "zkconnect";
    }

    public static class TopologyType {
        public static final String ALL = "all";
        public static final String DISPATCHER = "dispatcher";
        public static final String APPENDER = "appender";
    }

    /**
     * zookeeper路径常量
     */
    public static class ZKPath {
        /**
         * 在zookeeper中的根目录
         */
        public static final String ZK_ROOT = "/DBus";

        /**
         * zookeeper中TOPOLOGY存放的跟的路径
         */
        public static final String ZK_TOPOLOGY_ROOT = ZK_ROOT + "/Topology";

        /** zookeeper中Commons路径 */
        public static final String ZK_COMMONS = ZK_ROOT + "/Commons";
        /**
         * 接收到 meta sync 命令后暂停发送消息到 wormhole 的 data table 列表
         * 该常量是一个节点名字并不是一个zk的绝对路径
         */
        public static final String ZK_TERMINATED_TABLES = "termination_topics";

        /**
         * 被拉全量请求暂停的topic partition集合
         */
        public static final String ZK_FULLDATA_REQ_PARTITIONS = "fulldata_request_partitions";

        /**
         * 重新加载或者启动时用来控制kafka consumer跳过现有数据的设置
         */
        public static final String SPOUT_KAFKA_CONSUMER_NEXTOFFSET = "spout_kafka_consumer_nextoffset";
    }

    /**
     * 定义Emit Filed
     */
    public static class EmitFields {
        public static final String DATA = "data";
        public static final String COMMAND = "command";

        public static final String GROUP_FIELD = "group_field";
        public static final String EMIT_TYPE = "emit_type";

        /** 根据DbusGrouping定义, Emit 数据列表中最后一个值为 0 时会发送给所有下游blot */
        public static final int EMIT_TO_ALL = 0;
        /** 根据DbusGrouping定义, Emit 数据列表中最后一个值为非1值时会发送下游的一个blot */
        public static final int EMIT_TO_BOLT = 1;
        /** 根据DbusGrouping定义, Emit 数据列表中最后一个值为值2时会,预处理group field再进行分组发送 */
        public static final int EMIT_HEARTBEAT = 2;

        public static final String HEARTBEAT_FIELD_SUFFIX = ".heartbeat";
    }

    public static class OperationTypes {
        public static String INSERT = "I";
    }

    /**
     * 解析后消息的通用schema名
     */
    public static final String GENERIC_SCHEMA = "generic_wrapper";
    public static final int MAGIC_BYTE = 0x0;
    public static final String END = "end";
    public static final String BEGIN = "begin";
    public static final String NONE = "none";



    /**
     * mysql partition table default name
     */
    public static final String PARTITION_TABLE_DEFAULT_NAME = "0";
}
