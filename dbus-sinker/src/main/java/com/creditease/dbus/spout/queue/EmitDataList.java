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


package com.creditease.dbus.spout.queue;

import com.creditease.dbus.commons.DBusConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class EmitDataList {

    private List<DBusConsumerRecord<String, byte[]>> dataList;
    private Integer size;
    private Long time;

    public EmitDataList(DBusConsumerRecord<String, byte[]> record) {
        this.dataList = new ArrayList<>();
        dataList.add(record);
        this.size = record.serializedValueSize();
        this.time = System.currentTimeMillis();
    }

    public void add(DBusConsumerRecord<String, byte[]> record) {
        dataList.add(record);
        size += record.serializedValueSize();
    }

    public List<DBusConsumerRecord<String, byte[]>> getDataList() {
        return dataList;
    }

    public Integer getSize() {
        return size;
    }

    public Long getTime() {
        return time;
    }

    public void clear() {
        dataList.clear();
        size = 0;
        time = System.currentTimeMillis();
    }
}
