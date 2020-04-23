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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.wrapper;

import com.alibaba.fastjson.JSON;
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
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Strings;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Shrimp on 16/7/4.
 */
public class OraWrapperDefaultHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Set<String> noorderKeys = Constants.MessageBodyKey.noorderKeys;
    private CommandHandlerListener listener;

    private Tuple tuple;
    private MetaVersion version;
    private long offset;
    private String groupId;
    private DataTable table;

    public OraWrapperDefaultHandler(CommandHandlerListener listener) {
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

        List<GenericRecord> dataList = emitData.get(EmitData.MESSAGE);
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

        for (GenericRecord data : dataList) {
            PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord(data, noorderKeys);

            try {
                // 遇到update类型的消息，需要先处理b消息，之后再按原来的程序流程走
                if (wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString().toLowerCase().equals("u") && this.table.getOutputBeforeUpdateFlg() != 0) {
                    PairWrapper<String, Object> beforeWrapper = BoltCommandHandlerHelper.convertAvroRecordUseBeforeMap(data, noorderKeys);
                    // 生成meta信息
                    List<Object> beforePayloads = new ArrayList<>();
                    beforePayloads.add(generateUmsId(beforeWrapper.getProperties(Constants.MessageBodyKey.POS).toString()));
                    beforePayloads.add(beforeWrapper.getProperties(Constants.MessageBodyKey.OP_TS));
                    beforePayloads.add(Constants.UmsMessage.BEFORE_UPDATE_OPERATION);
                    beforePayloads.add(generateUmsUid());
                    payloadSize += addPayloadColumns(meta, beforeWrapper, decode, beforePayloads, data);
                    builder.appendPayload(beforePayloads.toArray());
                    payloadCount++;
                }

                // 生成meta信息
                List<Object> payloads = new ArrayList<>();
                payloads.add(generateUmsId(wrapper.getProperties(Constants.MessageBodyKey.POS).toString()));
                payloads.add(wrapper.getProperties(Constants.MessageBodyKey.OP_TS));
                payloads.add(wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString().toLowerCase());
                payloads.add(generateUmsUid());
                payloadSize += addPayloadColumns(meta, wrapper, decode, payloads, data);
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
                logger.error("[{}]Build dbus message of table[{}.{}] error, abort this message.\noriginal value:\n{}\nPairWrapper value: \n{}\nmessage: {}",
                        errId, version.getSchema(), version.getTable(), data.toString(), JSON.toJSONString(wrapper), e.getMessage(), e);
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

    private String generateUmsId(String pos) {
        try {
            Properties properties = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
            Long compensation = Long.parseLong(properties.getOrDefault(Constants.ConfigureKey.LOGFILE_NUM_COMPENSATION, 0).toString());
            if (compensation == null || compensation == 0) {
                return pos;
            }
            String umsId = Utils.oracleUMSID(pos, compensation);
            logger.debug("data logfile.number.compensation:{}", compensation);
            return umsId;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String generateUmsUid() throws Exception {
        //生成ums_uid
        return String.valueOf(listener.getZkService().nextValue(Utils.join(".", Utils.getDatasource().getDsName(), Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_SCHEMA, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_TABLE, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_VERSION)));
    }

    private int addPayloadColumns(MetaWrapper meta, PairWrapper<String, Object> wrapper, int decode, List<Object> payloads, GenericRecord data) throws Exception {
        int payloadSize = 0;
        for (MetaWrapper.MetaCell cell : meta.getColumns()) {
            if (cell.isSupported()) {
                com.creditease.dbus.stream.common.appender.utils.Pair<String, Object> pair = wrapper.getPair(cell.getColumnName()); // 这里使用替换特殊字符后的列获取oracle发送过来的数据
                Object value = pair == null ? null : pair.getValue();
                // 处理char(n char)的问题，
                // 备注：char(n char)类型的数据会被oracle使用base64编码，这里进行解码
                if (value != null && cell.getDataType().equalsIgnoreCase("CHAR") && "C".equals(cell.getCharUsed())) {
                    String valueStr = value.toString();
                    if (decode == 1 && !Strings.isNullOrEmpty(valueStr)) {
                        value = new String(Base64.getDecoder().decode(valueStr.getBytes()));
                        logger.debug("schema:{} table:{},column:{} was encoded with base64 value: {}, decoded value: {}",
                                version.getSchema(), version.getTable(), cell.getOriginalColumnName(), valueStr, value);
                    }
                }

                // 这里处理字段默认值的问题，解决oracle添加带有默认值的字段，但现有表中现有记录的日志数据中并没有该字段的值的问题
                // 判断字是否有默认值，如果有默认值则赋值给value(null)
                if (value == null && wrapper.getMissingFields().contains(cell.getColumnName())) {
//                if (value == null && wrapper.getMissingFields().contains(cell.getColumnName())) {
                    String defaultValue = cell.getDefaultValue();
                    try {
                        if (defaultValue != null) {
                            DataType type = DataType.convert(cell.getDataType(), cell.getDataPrecision(), cell.getDataScale());
                            // 不支持date类型的默认值
                            //TODO alter table t1 modify test date default to_date('2017-09-09','yyyy-mm-dd')
                            //TODO 对于test字段来讲，defaultValue的值为:to_date('2017-09-09','yyyy-mm-dd')而并不是我们期望的：2017-09-09，因此我们无法支持日志类型的默认值
                            if (type != DataType.DATE && type != DataType.DATETIME) {
                                defaultValue = defaultValue.trim();
                                if (defaultValue.startsWith("'")) {
                                    defaultValue = defaultValue.substring(1);
                                }
                                if (defaultValue.endsWith("'")) {
                                    defaultValue = defaultValue.substring(0, defaultValue.length() - 1);
                                }
                                value = DataType.convertValueByDataType(type, defaultValue.trim());
                            } else {
                                logger.warn("default value of {} and {} are not supported. Field:{}", DataType.DATE, DataType.DATETIME, cell.getColumnName());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Unsupported default. Field: {}.{}.{}, default value:{}", cell.getOwner(), cell.getTableName(), cell.getColumnName(), defaultValue);
                    }
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
            if (cell.isSupported()) {
                // 这里使用oralce原始列名作为ums schema中的字段名
                builder.appendSchema(cell.getOriginalColumnName(), DataType.convert(cell.getDataType(), cell.getDataPrecision(), cell.getDataScale()), cell.isNullable());
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
