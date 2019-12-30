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


package com.creditease.dbus.stream.common.appender.bean;

//import org.apache.storm.tuple.Tuple;

import java.beans.Transient;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Shrimp on 16/6/24.
 */
public class EmitData implements Serializable {
    public static final String NO_VALUE = "_no_value_";
    public static final String AVRO_SCHEMA = "avro_schema";
    public static final String MESSAGE = "message";
    public static final String VERSION = "version";
    public static final String DB_SCHEMA = "db_schema";
    public static final String TOPIC = "topic";
    public static final String DATA_TABLE = "data_table";
    public static final String OFFSET = "offset";
    public static final String GENERIC_DATA_LIST = "generic_data_list";
    public static final String STATUS = "status";
    public static final String CTRL_CMD = "ctrl_cmd";
    public static final String GROUP_KEY = "group_key";
    public static final String CURRENT_TIME_MS = "current_time_ms";

    private Map<String, Object> data;

    public EmitData() {
        data = new HashMap<>();
    }

    @Transient
    public void add(String key, Object val) {
        data.put(key, val);
    }

    @Transient
    public <T> T get(String key) {
        return (T) data.get(key);
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getStringValue(String key) {
        return data.containsKey(key) ? data.get(key).toString() : null;
    }
}
