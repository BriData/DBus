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


package com.creditease.dbus.tools;

import com.creditease.dbus.commons.Constants;

public class SinkerConstants {

    public static final String SINKER_TYPE = "SINKER_TYPE";

    /**
     * zk node name
     */
    public static final String CONFIG = "config.properties";
    public static final String CONSUMER = "consumer.properties";
    public static final String PRODUCER = "producer.properties";
    public static final String HDFS_CONFIG = "hdfs-config.properties";
    public static final String MYSQL_CONFIG = Constants.SINKER_ROOT + "/mysql-config.properties";

    /**
     * hdfs-config.properties
     */
    public static final String HDFS_URL = "hdfs.url";
    public static final String CORE_SITE = "core.site";
    public static final String HDFS_SITE = "hdfs.site";
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    public static final String HDFS_FILE_SIZE = "hdfs.file.size";
    public static final String HDFS_ROOT_PATH = "hdfs.root.path";
    public static final String HDFS_HSYNC_INTERVALS = "hdfs.hsync.intervals";

    /**
     * config.properties
     */
    public static final String SPOUT_MAX_FLOW_THRESHOLD = "spout.max.flow.threshold";
    public static final String COMMIT_INTERVALS = "commit.intervals";
    public static final String MANAGE_INTERVALS = "manage.intervals";
    public static final String EMIT_SIZ = "emit.size";
    public static final String EMIT_INTERVALS = "emit.intervals";
    public static final String STAT_TOPIC = "stat.topic";
    public static final String SINKER_HEARTBEAT_TOPIC = "sinker.heartbeat.topic";
    public static final String SINKER_TOPIC_LIST = "sinker.topic.list";
    public static final String STORM_KAFKA_READ_SPOUT_PARALLEL = "storm.kafka.read.spout.parallel";
    public static final String STORM_WRITE_BOUT_PARALLEL = "storm.write.bout.parallel";
    public static final String STORM_NUM_WORKERS = "storm.num.workers";
    public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
    public static final String STORM_MESSAGE_TIMEOUT = "storm.message.timeout";
    public static final String STORM_MAX_SPOUT_PENDING = "storm.max.spout.pending";

}
