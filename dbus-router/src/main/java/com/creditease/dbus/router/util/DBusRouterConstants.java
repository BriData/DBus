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


package com.creditease.dbus.router.util;

/**
 * Created by mal on 2018/5/22.
 */
public class DBusRouterConstants {

    private DBusRouterConstants() {
    }

    public static final String DBUS_ROUTER_DB_KEY = "dbus.router.db.key";

    /**
     * # router config
     * router.topic.offset=[{"topic": "test.topic", "offsetParis": "0->1357,1->3721"}]
     */
    public static final String TOPIC_OFFSET = "router.topic.offset";

    /**
     * # stat config
     * stat.topic=dbus_statistic
     * stat.url=dbus-kafka1.jishu.idc:9092,dbus-kafka2.jishu.idc:9092,dbus-kafka3.jishu.idc:9092
     */
    public static final String STAT_TOPIC = "stat.topic";
    public static final String STAT_URL = "stat.url";

    /**
     * # storm config
     * storm.message.timeout=10
     * storm.max.spout.pending=100
     * storm.num.workers=1
     * storm.kafka.read.spout.parallel=1
     * storm.encode.bolt.parallel=4
     * storm.kafka.write.bolt.parallel=1
     */
    public static final String STORM_MESSAGE_TIMEOUT = "storm.message.timeout";
    public static final String STORM_MAX_SPOUT_PENDING = "storm.max.spout.pending";
    public static final String STORM_NUM_WORKS = "storm.num.workers";
    public static final String STORM_TOPOLOGY_WORKER_CHILDOPTS = "storm.topology.worker.childopts";
    public static final String STORM_KAFKA_READ_SPOUT_PARALLEL = "storm.kafka.read.spout.parallel";
    public static final String STORM_ENCODE_BOLT_PARALLEL = "storm.encode.bolt.parallel";
    public static final String STORM_KAFKA_WREIT_BOLT_PARALLEL = "storm.kafka.write.bolt.parallel";

    public static final String PROJECT_TOPOLOGY_STATUS_START = "running";

    public static final String PROJECT_TOPOLOGY_TABLE_STATUS_START = "running";
    public static final String PROJECT_TOPOLOGY_TABLE_STATUS_CHANGED = "changed";
    public static final String PROJECT_TOPOLOGY_TABLE_STATUS_STOP = "stopped";

    /**
     * 标记表结构变更表使用
     */
    public static final int SCHEMA_CHANGE_TABLE_NONE = 0;
    public static final int SCHEMA_CHANGE_TABLE_UPDATE = 1;

    /**
     * 标记表结构变更字段变化使用
     */
    public static final int SCHEMA_CHANGE_COLUMN_NONE = 0;
    public static final int SCHEMA_CHANGE_COLUMN_DELETE = 1;
    public static final int SCHEMA_CHANGE_COLUMN_TYPE_UPDATE = 2;
    public static final int SCHEMA_CHANGE_COLUMN_ADD = 3;

}
