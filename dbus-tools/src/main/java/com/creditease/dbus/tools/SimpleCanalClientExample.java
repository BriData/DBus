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


/**
 * Created by dongwang47 on 2016/8/11.
 */


package com.creditease.dbus.tools;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleCanalClientExample {


    // 输入参数: dbus-n1  11111 testdb
    public static void main(String args[]) {
        //args = new String[]{"vdbus-4", "10000", "mysql_db2"};
        args = new String[]{"vdbus-4", "10000", "mysql_db2"};
        if (args.length != 3) {
            System.out.println("args: dbus-n1 11111 testdb");
            return;
        }
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        String dbname = args[2];

        // 创建链接
        CanalConnector connector = null;
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, port), dbname, "", "");
            //connector = CanalConnectors.newClusterConnector("vdbus-7:2181/DBus/Canal/mysql_db1", dbname, "", "");
            connector.connect();
            connector.subscribe("");
            connector.rollback();
            int totalEmtryCount = 120;
            while (emptyCount < totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();

                if (batchId == -1 || size == 0) {
                    emptyCount++;

                    System.out.print(".");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);


                    System.out.println("");
                    printEntry(message.getEntries(), batchId);
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            if (connector != null) {
                connector.disconnect();
            }
        }
    }

    private static void printEntry(List<Entry> entries, long batchId) {

        //       CanalPacket.Messages.Builder builder = CanalPacket.Messages.newBuilder();
        //       builder.setBatchId(batchId);

        for (Entry entry : entries) {

            //System.out.printf("entrytype: %s, entry size: %d\n", entry.getEntryType().toString(), entry.getSerializedSize());

//            builder.addMessages(entry.toByteString());

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            String namespace = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();

            System.out.println(namespace + ": header.eventType:" + entry.getHeader().getEventType());
            System.out.println(namespace + ": header.getEventLength:" + entry.getHeader().getEventLength());
            System.out.println(namespace + ": header.getServerId:" + entry.getHeader().getServerId());
            System.out.println(namespace + ": header.getVersion:" + entry.getHeader().getVersion());
            System.out.println(namespace + ": header.getPropsCount:" + entry.getHeader().getPropsCount());

            if (entry.getHeader().getPropsCount() > 0) {
                System.out.println(namespace + ": header.getHeader().getProps(0).getKey():" + entry.getHeader().getProps(0).getKey());
                System.out.println(namespace + ": header.getHeader().getProps(0).getValue():" + entry.getHeader().getProps(0).getValue());
            }


            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }


            EventType eventType = rowChage.getEventType();
            System.out.println(String.format(namespace + ": ================> binlog[%s:%s] , name[%s,%s] , eventType : %s, header.eventType: %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType, entry.getHeader().getEventType()));
            System.out.println(namespace + ": sql:" + rowChage.getSql());

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(namespace, rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(namespace, rowData.getAfterColumnsList());
                } else if (eventType == EventType.UPDATE) {
                    System.out.println(namespace + ": -------> before");
                    printColumn(namespace, rowData.getBeforeColumnsList());
                    System.out.println(namespace + ": -------> after");
                    printColumn(namespace, rowData.getAfterColumnsList());
                } else {
                    ;
                }
            }
        }

//        CanalPacket.Messages msgsOld = builder.build();
//        byte[] byteArray  = msgsOld.toByteArray();
//
//        try {
//            CanalPacket.Messages msgs = CanalPacket.Messages.parseFrom(byteArray);
//            List<ByteString> list = msgs.getMessagesList();
//            for (ByteString str : list) {
//                Entry ent = CanalEntry.Entry.parseFrom(str);
//                System.out.println(ent.getSerializedSize());
//            }
//        }catch (InvalidProtocolBufferException ex) {
//            System.out.printf(ex.getMessage());
//        }

//        byte[] byteArray = output.toByteArray();
//        System.out.printf("byteArray : %d\n", byteArray.length);
//
//        InputStream input = new ByteArrayInputStream(byteArray);
//        Entry entry = null;
//
//        List<Entry> list = new ArrayList<>();
//        try {
//            // 反序列化
//            while (input.available() != 0) {
//                entry = CanalEntry.Entry.parseFrom(input);
//                list.add(entry);
//                System.out.printf("skip : %d\n", entry.getSerializedSize());
//                //input.skip(entry.getSerializedSize());
//           }
//        }catch (IOException ex) {
//            System.out.printf(ex.getMessage());
//            return;
//        }

    }

    private static void printColumn(String namespace, List<Column> columns) {
        for (Column column : columns) {
            System.out.println(namespace + ": " + column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()
                    + ", sqltype=" + column.getMysqlType() + ", isnull=" + column.getIsNull());
        }
    }
}
