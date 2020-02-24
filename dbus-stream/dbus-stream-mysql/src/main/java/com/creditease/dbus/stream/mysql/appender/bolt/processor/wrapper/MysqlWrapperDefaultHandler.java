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

package com.creditease.dbus.stream.mysql.appender.bolt.processor.wrapper;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.msgencoder.EncodeColumnProvider;
import com.creditease.dbus.msgencoder.PluggableMessageEncoder;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.msgencoder.UmsEncoder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.CachedEncodeColumnProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Pair;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.protobuf.Support;
import com.creditease.dbus.stream.mysql.appender.protobuf.convertor.Convertor;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MsgColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.creditease.dbus.stream.common.Constants.ConfigureKey.MYSQL_UMS_WITH_TABLE_PARTITION;

public class MysqlWrapperDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private CommandHandlerListener listener;
    private long lastPos = 0L;
    private Tuple tuple;
    private MetaVersion version;
    private long offset;
    private String groupId;
    private DataTable table;

    public MysqlWrapperDefaultHandler(CommandHandlerListener listener) {

        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        MetaVersion version = emitData.get(EmitData.VERSION);

        this.tuple = tuple;
        this.offset = emitData.get(EmitData.OFFSET);
        this.version = emitData.get(EmitData.VERSION);
        this.groupId = groupField(version.getSchema(), version.getTable());
        this.table = emitData.get(EmitData.DATA_TABLE);

        List<MessageEntry> dataList = emitData.get(EmitData.MESSAGE);

        int payloadCount = 0;
        int payloadSize = 0;
        int payloadMaxCount = getMaxCount();
        int payloadMaxSize = getMaxSize();
        DbusMessageBuilder builder = createBuilderWithSchema(version, dataList.get(0));

        EntryHeader header;
        long uniquePos = 0;

        // 脱敏
        EncodeColumnProvider provider = new CachedEncodeColumnProvider(version.getTableId());
        final MetaVersion v = version;
        UmsEncoder encoder = new PluggableMessageEncoder(PluginManagerProvider.getManager(), (e, column, message) -> {
            BoltCommandHandlerHelper.onEncodeError(message, v, column, e);
        });
        for (MessageEntry msgEntry : dataList) {
            header = msgEntry.getEntryHeader();
            header.setLastPos(lastPos);
            MsgColumn msgCol = msgEntry.getMsgColumn();
            for (RowData rowData : msgCol.getRowDataLst()) {
                try {
                    lastPos = Long.parseLong(header.getPos()) + (++uniquePos);
                    // 对UPDATE类型的增量数据特殊处理，增加b类型的tuple，在同一个payload里
                    if (header.isUpdate() && this.table.getOutputBeforeUpdateFlg() != 0) {
                        PairWrapper<String, Object> beforeWrapper = Convertor.convertProtobufRecordBeforeUpdate(msgEntry.getEntryHeader(), rowData);
                        List<Object> beforePayload = new ArrayList<>();
                        beforePayload.add(header.getPos()); // ums_id
                        beforePayload.add(header.getTsTime()); // ums_ts
                        beforePayload.add(Constants.UmsMessage.BEFORE_UPDATE_OPERATION);// ums_op = "b"
                        beforePayload.add(generateUmsUid());// ums_uid
                        List<Column> beforeColumns = rowData.getBeforeColumnsList();
                        payloadSize += addPayloadColumns(beforePayload, beforeColumns, beforeWrapper, builder);
                        builder.appendPayload(beforePayload.toArray());
                        payloadCount++;
                    }

                    PairWrapper<String, Object> wrapper = Convertor.convertProtobufRecord(msgEntry.getEntryHeader(), rowData);
                    List<Object> payloads = new ArrayList<>();
                    payloads.add(header.getPos()); // ums_id
                    payloads.add(header.getTsTime()); // ums_ts
                    payloads.add(Support.getOperTypeForUMS(header.getOperType()));// ums_op
                    payloads.add(generateUmsUid());// ums_uid
                    List<Column> columns = Support.getFinalColumns(header.getOperType(), rowData);
                    payloadSize += addPayloadColumns(payloads, columns, wrapper, builder);
                    builder.appendPayload(payloads.toArray());
                    payloadCount++;

                    // 判断消息payload数量和大小是否超出限制
                    if (payloadCount >= payloadMaxCount) {
                        logger.debug("Payload count out of limitation[{}]!", payloadMaxCount);
                        encoder.encode(builder.getMessage(), provider);
                        emitMessage(builder.getMessage());
                        builder = createBuilderWithSchema(version, dataList.get(0));
                        payloadCount = 0;
                    } else if (payloadSize >= payloadMaxSize) {
                        logger.debug("Payload size out of limitation[{}]!", payloadMaxSize);
                        encoder.encode(builder.getMessage(), provider);
                        emitMessage(builder.getMessage());
                        builder = createBuilderWithSchema(version, dataList.get(0));
                        payloadSize = 0;
                    }
                } catch (Exception e) {
                    long errId = System.currentTimeMillis();
                    logger.error("[%s]Build dbus message error, abort this message, {}", errId + "", e.getMessage(), e);
                    BoltCommandHandlerHelper.onBuildMessageError(errId + "", version, e);
                }
            }
        }
        // 判断message是否包含payload,如果payload列表为空则不写kafka
        DbusMessage message = builder.getMessage();

        encoder.encode(message, provider);

        if (!message.getPayload().isEmpty()) {
            emitMessage(builder.getMessage());
        }
    }

    private String generateUmsUid() throws Exception {
        //生成ums_uid
        return String.valueOf(listener.getZkService().nextValue(Utils.join(".", Utils.getDatasource().getDsName(), Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_SCHEMA, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_TABLE, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_VERSION)));
    }

    private int addPayloadColumns(List<Object> payloads, List<Column> columns,
                                  PairWrapper<String, Object> wrapper, DbusMessageBuilder builder) throws Exception {
        int payloadSize = 0;
        //Map<String, Column> map = new HashMap<>();
        //for (Column column : columns) {
        //    map.put(column.getName(), column);
        //}
        List<DbusMessage.Field> fields = builder.getMessage().getSchema().getFields();

        for (int i = builder.getUmsFixedFields(); i < fields.size(); i++) {
            DbusMessage.Field field = fields.get(i);
            // 这里避免，源端表中包含ums_id_/ums_ts_等字段
            //Column column = map.get(field.getName());
            //if (column != null && Support.isSupported(column)) {
                Pair<String, Object> pair = wrapper.getPair(field.getName());
                Object value = pair.getValue();
                payloads.add(value);
                if (value != null) {
                    payloadSize += value.toString().getBytes("utf-8").length;
                }
            //}
        }
        return payloadSize;
    }

    /**
     * 创建DBusMessageBuilder对象,同时生成ums schema
     */
    private DbusMessageBuilder createBuilderWithSchema(MetaVersion version, MessageEntry msgEntry) {
        DbusMessageBuilder builder = new DbusMessageBuilder();
        String namespace;
        if (outputTablePartition()) {
            namespace = builder.buildNameSpace(Utils.getDataSourceNamespace(),
                    msgEntry.getEntryHeader().getSchemaName(), msgEntry.getEntryHeader().getTableName(),
                    version.getVersion(), msgEntry.getEntryHeader().getPartitionTableName());
        } else {
            namespace = builder.buildNameSpace(Utils.getDataSourceNamespace(),
                    msgEntry.getEntryHeader().getSchemaName(), msgEntry.getEntryHeader().getTableName(),
                    version.getVersion());
        }
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId());
        EventType eventType = msgEntry.getEntryHeader().getOperType();
        RowData rowData = msgEntry.getMsgColumn().getRowDataLst().get(0);
        List<Column> columns = Support.getFinalColumns(eventType, rowData);
        for (Column column : columns) {
            String colType = Support.getColumnType(column);
            // 这里避免，源端表中包含ums_id_/ums_ts_等字段
            if (Support.isSupported(colType) && !builder.getMessage().containsFiled(column.getName())) {
                builder.appendSchema(column.getName(), DataType.convertMysqlDataType(colType), true);
            }
        }
        return builder;
    }

    private boolean outputTablePartition() {
        Integer outputFlag = PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, MYSQL_UMS_WITH_TABLE_PARTITION);
        return outputFlag == null || outputFlag.equals(1);
    }

    private void emitMessage(DbusMessage message) {
        EmitData data = new EmitData();
        data.add(EmitData.MESSAGE, message);
        data.add(EmitData.VERSION, version);
        data.add(EmitData.GROUP_KEY, groupId);
        data.add(EmitData.OFFSET, offset);

        this.emit(listener.getOutputCollector(), tuple, groupId, data, Command.UNKNOWN_CMD);
    }

    private int getMaxCount() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE,
                Constants.ConfigureKey.UMS_PAYLOAD_MAX_COUNT);
    }

    private int getMaxSize() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE,
                Constants.ConfigureKey.UMS_PAYLOAD_MAX_SIZE);
    }

    public static void main(String[] args) {
        //offsetOperator = new JsonNodeOperator(zk, Utils.join("/", Utils.buildZKTopologyPath(topologyId), Constants.ZKPath.SPOUT_KAFKA_CONSUMER_NEXTOFFSET));
        // String type = "int(10)";
        String type = "int";
        System.out.println(StringUtils.substringBefore(type, "("));
       /* long pos = 0;
        pos = (Long) null;
        System.out.println(pos);*/
        long threadId = Thread.currentThread().getId();
        threadId = threadId << 1;
        System.out.println(threadId);
    }
}
