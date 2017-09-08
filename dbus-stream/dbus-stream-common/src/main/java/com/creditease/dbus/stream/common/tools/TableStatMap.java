/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.stream.common.tools;

import com.creditease.dbus.commons.StatMessage;

import java.util.HashMap;

/**
 * key = schemaName.tableName
 */
public class TableStatMap extends HashMap<String, StatMessage> {

    private String dsName;

    public void initDsName (String dsName) {
        this.dsName = dsName;
    }

    private String makeKey(String schemaName, String tableName) {
        return String.format("%s.%s", schemaName, tableName);
    }

    public void mark(String schemaName, String tableName, long count) {

        String key = makeKey(schemaName, tableName);
        StatMessage message = this.get(key);
        if (message == null) {
            return;
        }
        //count++
        message.addCount(count);
    }

    public StatMessage logMeter(String schemaName, String tableName, long checkpointMS, long txTimeMS) {

        String key = makeKey(schemaName, tableName);
        StatMessage message = this.get(key);
        if (message == null) {
            message = new StatMessage(dsName, schemaName, tableName, StatMessage.DISPATCH_TYPE);
            this.put(key, message);
        }

        message.setCheckpointMS(checkpointMS);
        message.setTxTimeMS(txTimeMS);
        message.setLocalMS(System.currentTimeMillis());

        return message;
    }
}
