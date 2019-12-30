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


package com.creditease.dbus.stream.mysql.appender.protobuf.parser;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.creditease.dbus.stream.mysql.appender.exception.ProtobufParseException;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * mysql binlog protobuf解析器
 *
 * @author xiongmao
 */
public class BinlogProtobufParser {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private static BinlogProtobufParser parser;

    private BinlogProtobufParser() {
    }

    public static BinlogProtobufParser getInstance() {
        if (parser == null) {
            synchronized (BinlogProtobufParser.class) {
                if (parser == null) {
                    parser = new BinlogProtobufParser();
                }
            }
        }
        return parser;
    }

    public List<EntryHeader> getHeader(byte[] input) throws Exception {
        CanalPacket.Messages msg = null;
        List<EntryHeader> list = new ArrayList<>();
        try {
            msg = CanalPacket.Messages.parseFrom(input);
            List<ByteString> lst = msg.getMessagesList();
            for (ByteString bs : lst) {
                Entry entry = Entry.parseFrom(bs);
                Header header = entry.getHeader();
                EntryHeader entryHeader = new EntryHeader(header);
                list.add(entryHeader);
            }
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (ProtobufParseException e) {
            throw e;
        }
        return list;
    }

    public List<MessageEntry> getEntry(byte[] input) throws Exception {
        CanalPacket.Messages msg = null;
        List<MessageEntry> list = new ArrayList<>();
        try {
            CanalPacket.Messages.Builder msgBuilder = CanalPacket.Messages.newBuilder();
            //	msg = CanalPacket.Messages.parseFrom(input);
            msg = msgBuilder.mergeFrom(input).build();
            List<ByteString> lst = msg.getMessagesList();
            for (ByteString bs : lst) {
                Entry.Builder entryBuilder = Entry.newBuilder();
                Entry entry = entryBuilder.mergeFrom(bs).build();
                //	Entry entry = CanalEntry.Entry.parseFrom(bs);
                MessageEntry msgEntry = new MessageEntry(entry);
                list.add(msgEntry);
                entryBuilder.clear();
            }
            msgBuilder.clear();
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (ProtobufParseException e) {
            throw e;
        }
        return list;
    }
}
