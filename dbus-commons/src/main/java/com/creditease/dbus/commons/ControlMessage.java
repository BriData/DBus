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


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ControlMessage implements Serializable {

    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";
    public static final String FROM = "from";
    public static final String PAYLOAD = "payload";

    private long id;
    private String type;
    private String timestamp;
    private String from;
    private Map<String, Object> payload;
    private Map<String, Object> project;

    public static ControlMessage parse(String jsonString) {
        return JSON.parseObject(jsonString, ControlMessage.class);
    }

    public ControlMessage() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        this.timestamp = sdf.format(date);
        this.payload = new HashMap<>();
    }

    public ControlMessage(long id, String type, String from) {
        this(id, type, from, new HashMap<>(), new HashMap<>());
    }

    public ControlMessage(long id, String type, String from, Map<String, Object> payload, Map<String, Object> project) {

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        this.timestamp = sdf.format(date);

        this.id = id;
        this.type = type;
//        this.timestamp = timestamp;
        this.from = from;
        this.payload = payload;
        this.project = project;

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public void addPayload(String key, Object value) {
        this.payload.put(key, value);
    }

    public <T> T payloadValue(String key, Class<T> clazz) {
        Object val = payload.get(key);
        if (val != null && clazz != null && clazz.isInstance(val)) {
            return (T) val;
        }
        return null;
    }

    public Map<String, Object> getProject() {
        return project;
    }

    public void setProject(Map<String, Object> project) {
        this.project = project;
    }

    public String toJSONString() {
        return JSON.toJSONString(this);
    }


    public static void main(String[] args) {
        ControlMessage msg = new ControlMessage(12, ControlType.DISPATCHER_RELOAD_CONFIG.name(), "who");
        msg.addPayload("key1", "value1");
        String jsonString = msg.toJSONString();

        ControlMessage obj = ControlMessage.parse(jsonString);
    }
}
