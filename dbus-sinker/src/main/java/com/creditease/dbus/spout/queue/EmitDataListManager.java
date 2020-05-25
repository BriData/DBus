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
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EmitDataListManager {

    private Map<String, EmitDataList> dataMap;
    private Integer emitSize;
    private Integer emitIntervals;

    public EmitDataListManager(Integer emitSize, Integer emitIntervals) {
        this.dataMap = new HashMap<>();
        this.emitSize = emitSize;
        this.emitIntervals = emitIntervals;
    }

    public void emit(String key, DBusConsumerRecord<String, byte[]> record) {
        EmitDataList list = dataMap.get(key);
        if (list == null) {
            list = new EmitDataList(record);
            dataMap.put(key, list);
        } else {
            list.add(record);
        }
    }

    public List<EmitDataList> getEmitDataList() {
        return dataMap.values().stream().filter(list -> list.getSize() >= emitSize
                || (System.currentTimeMillis() - list.getTime()) >= emitIntervals
                || list.getDataList().size() > 0).collect(Collectors.toList());
    }

    public List<EmitDataList> getAllEmitDataList() {
        return dataMap.values().stream().filter(list -> list.getSize() > 0).collect(Collectors.toList());
    }

    public void remove(String key) {
        dataMap.remove(key);
    }

    public void removeAll() {
        dataMap.clear();
    }

    private String bildKey(DBusConsumerRecord<String, byte[]> record) {
        String[] vals = StringUtils.split(record.key(), ".");
        return StringUtils.joinWith(".", vals[2], vals[3], vals[4]);
    }
}
