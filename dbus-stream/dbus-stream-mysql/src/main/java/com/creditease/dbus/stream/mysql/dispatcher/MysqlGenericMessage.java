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


package com.creditease.dbus.stream.mysql.dispatcher;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.creditease.dbus.stream.common.tools.IGenericMessage;

/**
 * Created by dongwang47 on 2016/8/19.
 */
public class MysqlGenericMessage implements IGenericMessage {

    private CanalEntry.Entry entry;

    public MysqlGenericMessage(CanalEntry.Entry entry) {
        this.entry = entry;
    }

    @Override
    public String getNameSpace() {
        return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
    }

    @Override
    public String getSchemaName() {
        return entry.getHeader().getSchemaName();
    }

    @Override
    public String getTableName() {
        String tableName = entry.getHeader().getTableName();
        if (tableName.indexOf(".") > 0) {
            tableName = tableName.split("\\.")[0];
        }
        return tableName;
    }

    @Override
    public int getSchemaId() {
        return 0;
    }

    @Override
    public int getSchemaHash() {
        //fake hash
        return 0x12345678;
    }

    @Override
    public int getRowCount() {
        //int proCount = entry.getHeader().getPropsCount();
        //if (proCount == 1) {
        //    CanalEntry.Pair pair = entry.getHeader().getProps(0);
        //    if (pair.getKey().equals("rowCount"))
        //        return Integer.parseInt(pair.getValue());
        //}
        for (CanalEntry.Pair pair : entry.getHeader().getPropsList()) {
            if (pair.getKey().equals("rowsCount")) {
                return Integer.parseInt(pair.getValue());
            }
        }

        return 1;
    }

    @Override
    public boolean isDML() {
        CanalEntry.EventType eventType = entry.getHeader().getEventType();
        switch (eventType) {
            case INSERT:
            case UPDATE:
            case DELETE:
                return true;
            default:
                return false;
        }
    }

    public CanalEntry.Entry getEntry() {
        return entry;
    }
}
