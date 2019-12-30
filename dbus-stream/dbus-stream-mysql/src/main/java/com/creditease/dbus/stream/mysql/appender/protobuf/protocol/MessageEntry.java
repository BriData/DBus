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

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.creditease.dbus.stream.mysql.appender.exception.ProtobufParseException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Serializable;

public class MessageEntry implements Serializable {
    private Entry entry;
    private RowChange rowChange;
    private EntryHeader entryHeader;
    private MsgColumn msgColumn;

    public MessageEntry(Entry entry) {
        msgColumn = new MsgColumn();
        try {
            RowChange.Builder rowBuilder = RowChange.newBuilder();
            rowChange = rowBuilder.mergeFrom(entry.getStoreValue()).build();
            //	rowChange = RowChange.parseFrom(entry.getStoreValue());
            msgColumn.setRowDataLst(rowChange.getRowDatasList());
            Header header = entry.getHeader();
            entryHeader = new EntryHeader(header);
            rowBuilder.clear();
        } catch (InvalidProtocolBufferException e) {
            throw new ProtobufParseException("protobuf parser rowChange error!");
        } catch (ProtobufParseException e) {
            throw e;
        }
    }

    public EntryHeader getEntryHeader() {
        return entryHeader;
    }

    public boolean isDdl() {
        return rowChange.getIsDdl();
    }

    public String getSql() {
        return rowChange.getSql();
    }

    public MsgColumn getMsgColumn() {
        return msgColumn;
    }
}
