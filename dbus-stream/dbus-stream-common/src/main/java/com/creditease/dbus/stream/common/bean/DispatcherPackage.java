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


package com.creditease.dbus.stream.common.bean;

import java.io.Serializable;

/**
 * Created by dongwang47 on 2017/8/15.
 */
public class DispatcherPackage implements Serializable {
    private String key;
    private byte[] content;
    private int msgCount;
    private String schemaName;
    private String toTopic;

    public DispatcherPackage(String key, byte[] content, int msgCount, String schemaName, String toTopic) {
        this.key = key;
        this.content = content;
        this.msgCount = msgCount;
        this.schemaName = schemaName;
        this.toTopic = toTopic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getContent() {
        return content;
    }

    public int getMsgCount() {
        return msgCount;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getToTopic() {
        return toTopic;
    }
}
