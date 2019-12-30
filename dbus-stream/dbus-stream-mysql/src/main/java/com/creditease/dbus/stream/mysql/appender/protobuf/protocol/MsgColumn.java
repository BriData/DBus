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


package com.creditease.dbus.stream.mysql.appender.protobuf.protocol;

import com.alibaba.otter.canal.protocol.CanalEntry.RowData;

import java.io.Serializable;
import java.util.List;

public class MsgColumn implements Serializable {
    private List<RowData> rowDataLst;

    public MsgColumn() {
    }

    public List<RowData> getRowDataLst() {
        return rowDataLst;
    }

    public void setRowDataLst(List<RowData> rowDataLst) {
        this.rowDataLst = rowDataLst;
    }

    public int getCount() {
        return rowDataLst.size();
    }
}
