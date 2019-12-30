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


package com.creditease.dbus.log.processor.util;

/**
 * Created by Administrator on 2017/9/28.
 */
public class Constants {

    private Constants() {
    }

    public static final String EMIT_DATA_TYPE_CTRL = "control";

    public static final String EMIT_DATA_TYPE_NORMAL = "normal";

    public static final String EMIT_DATA_TYPE_HEARTBEAT = "heartbeat";

    public static final String EMIT_DATA_TYPE_STAT = "stat";

    public static final String DBUS_CONFIG_DB_KEY = "dbus.conf";

    public static final String TOPOLOGY_ID = "dbus.storm.topology.id";

    public static final String LOG_DS_NAME = "log.ds.name";

    public static final String LOG_OFFSET = "log.offset";

    public static final String LOG_MAX_EMIT_COUNT = "log.max.emit.count";

    public static final String LOG_SPEED_LIMIT_INTERVAL = "log.speed.limit.interval";

    public static final String LOG_KAFKA_READ_SPOUT_PARALLEL = "log.kafka.read.spout.parallel";

    public static final String LOG_TRANSFORM_BOLT_PARALLEL = "log.transform.bolt.parallel";

    public static final String LOG_STAT_KAFKA_WRITE_BOLT_PARALLEL = "log.stat.kafka.write.bolt.parallel";

    public static final String LOG_UMS_KAFKA_WRITE_BOLT_PARALLEL = "log.ums.kafka.write.bolt.parallel";

    public static final String LOG_KAFKA_WRITE_BOLT_PARALLEL = "log.kafka.write.bolt.parallel";

    public static final String LOG_MESSAGE_TIMEOUT = "log.message.timeout";

    public static final String LOG_MAXSPOUTPENDING = "log.maxSpoutPending";

    public static final String LOG_NUMWORKERS = "log.NumWorkers";

    public static final String LOG_HEARTBEAT_TYPE = "log.heartbeat.type";

}
