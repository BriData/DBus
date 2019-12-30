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


package com.creditease.dbus.stream.db2.appender.bolt.processor.wrapper;

import com.creditease.dbus.commons.*;
import com.creditease.dbus.msgencoder.*;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.CachedEncodeColumnProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.common.tools.DateUtil;
import com.creditease.dbus.stream.db2.appender.bolt.processor.vo.AppenderDataResults;
import com.creditease.dbus.stream.db2.appender.enums.DbusDb2DmlType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Db2WrapperDefaultHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Set<String> noorderKeys = Constants.MessageBodyKey.noorderKeys;
    private CommandHandlerListener listener;

    private Tuple tuple;
    private MetaVersion version;
    private long offset;
    private String groupId;
    private DataTable table;


    public Db2WrapperDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        this.tuple = tuple;
        this.offset = emitData.get(EmitData.OFFSET);
        this.version = emitData.get(EmitData.VERSION);
        this.groupId = groupField(version.getSchema(), version.getTable());
        this.table = emitData.get(EmitData.DATA_TABLE);
        MetaWrapper meta = version.getMeta();

        List<AppenderDataResults> dataList = emitData.get(EmitData.MESSAGE);
        logger.debug("[BEGIN] receive data,offset:{}, dataList.size: {}", offset, dataList.size());

        //*********************************************************************
        if (logger.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(version.getTable()).append(":");
            for (MetaWrapper.MetaCell cell : meta.getColumns()) {
                buf.append(cell.getColumnName()).append(",");
            }
            logger.debug("[fields-order] {} before build message: {}", offset, buf.toString());

            String fieldsOrderFromAppender = emitData.get("fields-order");
            if (fieldsOrderFromAppender != null && !fieldsOrderFromAppender.equals(buf.toString())) {
                logger.error("[fields-order] fields order not matched.\nappenderBolt:{}\nkafkaBolt:{}", fieldsOrderFromAppender, buf.toString());
            }
        }
        //***********************************************************************

        int payloadCount = 0;
        int payloadSize = 0;
        int payloadMaxCount = getMaxCount();
        int payloadMaxSize = getMaxSize();

        DbusMessageBuilder builder = createBuilderWithSchema(version, meta);

        int decode = PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, Constants.ConfigureKey.BASE64_DECODE);

        // 脱敏
        EncodeColumnProvider provider = new CachedEncodeColumnProvider(version.getTableId());
        List<EncodeColumn> encodeColumns = provider.getColumns();
        final MetaVersion v = version;
        UmsEncoder encoder = new PluggableMessageEncoder(PluginManagerProvider.getManager(), (e, column, message) -> {
            BoltCommandHandlerHelper.onEncodeError(message, v, column, e);
        });

        for (AppenderDataResults appenderDataResults : dataList) {
            GenericRecord data = appenderDataResults.getGenericRecord();
//            PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord(data, noorderKeys);

            try {
                // 遇到update类型的消息，需要先处理b消息，之后再按原来的程序流程走
//                if (wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString().toLowerCase().equals("u") && this.table.getOutputBeforeUpdateFlg() != 0) {
//                    PairWrapper<String, Object> beforeWrapper = BoltCommandHandlerHelper.convertAvroRecordUseBeforeMap(data, noorderKeys);
//                    // 生成meta信息
//                    List<Object> beforePayloads = new ArrayList<>();
//                    beforePayloads.add(beforeWrapper.getProperties(Constants.MessageBodyKey.POS));
//                    beforePayloads.add(beforeWrapper.getProperties(Constants.MessageBodyKey.OP_TS));
//                    beforePayloads.add(Constants.UmsMessage.BEFORE_UPDATE_OPERATION);
//                    beforePayloads.add(generateUmsUid());
//                    payloadSize += addPayloadColumns(meta, beforeWrapper, decode, beforePayloads, data);
//                    builder.appendPayload(beforePayloads.toArray());
//                    payloadCount++;
//                }

                // 生成meta信息
                List<Object> payloads = new ArrayList<>();
                String format = Constants.DB2MessageBodyKey.DB2_CURTMSTP_FORMAT;
                Long time = DateUtil.convertStrToLong4Date(data.get(Constants.DB2MessageBodyKey.DB2_CURTMSTP).toString(), format);
                String offset = appenderDataResults.getOffset();
//                Long umsId = getOffsetNumCompensation() + time + Long.parseLong(offset);
                Long umsId = getOffsetNumCompensation() + Long.parseLong(offset);

                payloads.add(umsId);
                payloads.add(DateUtil.convertLongToStr4Date(time));
                //db2中有几种DML类型: 1）PT:insert 2) DL:delete 3) UP:update
                String ddlType = DbusDb2DmlType.getDmlType(data.get(Constants.DB2MessageBodyKey.DB2_ENTTYP).toString());
                if (ddlType.equals(null))
                    logger.error("DB2_ENTTYP is error: [" + data.get(Constants.DB2MessageBodyKey.DB2_ENTTYP).toString() + "] ");
                payloads.add(ddlType);
                payloads.add(generateUmsUid());
                payloadSize += addPayloadColumns(meta, payloads, data);
                builder.appendPayload(payloads.toArray());
                payloadCount++;

                // 判断消息payload数量和大小是否超出限制
                if (payloadCount >= payloadMaxCount) {
                    logger.debug("Payload count out of limitation[{}]!", payloadMaxCount);
                    encoder.encode(builder.getMessage(), encodeColumns);

                    emitMessage(builder.getMessage());

                    builder = createBuilderWithSchema(version, meta);
                    payloadCount = 0;
                } else if (payloadSize >= payloadMaxSize) {
                    logger.debug("Payload size out of limitation[{}]!", payloadMaxCount);
                    encoder.encode(builder.getMessage(), encodeColumns);

                    emitMessage(builder.getMessage());

                    builder = createBuilderWithSchema(version, meta);
                    payloadSize = 0;
                }
            } catch (Exception e) {
                long errId = System.currentTimeMillis();
                logger.error("[{}]Build dbus message of table[{}.{}] error, abort this message.\noriginal value:\n{}\nmessage: {}",
                        errId, version.getSchema(), version.getTable(), data.toString(), e.getMessage(), e);
                BoltCommandHandlerHelper.onBuildMessageError(errId + "", version, e);
            }
        }

        // 判断message是否包含payload,如果payload列表为空则不写kafka
        DbusMessage message = builder.getMessage();
        encoder.encode(message, encodeColumns);

        if (!message.getPayload().isEmpty()) {
            emitMessage(message);
        }
    }

    private String generateUmsUid() throws Exception {
        //生成ums_uid
        return String.valueOf(listener.getZkService().nextValue(Utils.join(".", Utils.getDatasource().getDsName(), Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_SCHEMA, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_TABLE, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_VERSION)));
    }

    private long getOffsetNumCompensation() {
        try {
            Properties properties = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
            return Long.parseLong(properties.getOrDefault(Constants.ConfigureKey.DB2_OFFSET_COMPENSATION, 0).toString());
        } catch (Exception e) {
            throw new RuntimeException("获取配置文件失败", e);
        }
    }

    private int addPayloadColumns(MetaWrapper meta, List<Object> payloads, GenericRecord data) throws Exception {
        int payloadSize = 0;
        for (MetaWrapper.MetaCell cell : meta.getColumns()) {
            if (cell.isSupportedOnDb2()) {
                Schema schema = data.getSchema();
                Schema.Field field = schema.getField(cell.getColumnName());
                Object value = null;
                try {
                    value = data.get(field.name());
                } catch (Exception e) {
                    logger.error("{}", e);
                    logger.info("schema:{}", schema.toString());
                    logger.info("data: {}", data.toString());
                }
                payloads.add(value);
                if (value != null) {
                    payloadSize += value.toString().getBytes("utf-8").length;
                }
            }
        }

        return payloadSize;
    }

    private void emitMessage(DbusMessage message) {
        EmitData data = new EmitData();
        data.add(EmitData.MESSAGE, message);
        data.add(EmitData.VERSION, version);
        data.add(EmitData.GROUP_KEY, groupId);
        data.add(EmitData.OFFSET, offset);

        this.emit(listener.getOutputCollector(), tuple, groupId, data, Command.UNKNOWN_CMD);
    }

    /**
     * 创建DBusMessageBuilder对象,同时生成ums schema
     */
    private DbusMessageBuilder createBuilderWithSchema(MetaVersion version, MetaWrapper meta) {
        DbusMessageBuilder builder = new DbusMessageBuilder();
        String namespace = builder.buildNameSpace(Utils.getDataSourceNamespace(), version.getSchema(), version.getTable(), version.getVersion());
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId());
        for (MetaWrapper.MetaCell cell : meta.getColumns()) {
            if (cell.isSupportedOnDb2()) {
                // 这里使用db2原始列名作为ums schema中的字段名
                builder.appendSchema(cell.getOriginalColumnName(), DataType.convertDb2DataType(cell.getDataType()), cell.isNullable());
            }
        }
        // 处理抓取的业务数据
        return builder;
    }

    private int getMaxCount() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, Constants.ConfigureKey.UMS_PAYLOAD_MAX_COUNT);
    }

    private int getMaxSize() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, Constants.ConfigureKey.UMS_PAYLOAD_MAX_SIZE);
    }
}
