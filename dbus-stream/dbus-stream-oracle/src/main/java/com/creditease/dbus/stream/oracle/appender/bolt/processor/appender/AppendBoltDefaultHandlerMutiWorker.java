/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.oracle.appender.avro.GenericData;
import com.creditease.dbus.stream.oracle.appender.avro.GenericSchemaDecoder;
import com.creditease.dbus.stream.oracle.appender.avro.SchemaProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 普通数据处理器
 * Created by Shrimp on 16/7/1.
 */
public class AppendBoltDefaultHandlerMutiWorker implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public AppendBoltDefaultHandlerMutiWorker(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple input) {
        EmitData emitData = (EmitData) input.getValueByField(Constants.EmitFields.DATA);
        Command emitCmd = (Command) input.getValueByField(Constants.EmitFields.COMMAND);
        List<GenericData> dataList = emitData.get(EmitData.MESSAGE);

        try {
            long offset = emitData.get(EmitData.OFFSET);
            logger.debug("[BEGIN] receive data,offset:{}", offset);

            if (dataList == null || dataList.isEmpty()) {
                return;
            }

            // dataList中存储同一个表的数据,并且数据的meta版本号相同
            GenericData data = dataList.get(0);
            int idx = data.getTableName().indexOf(".");
            String fullName = data.getTableName();
            String dbSchema = fullName.substring(0, idx);
            String tableName = fullName.substring(idx + 1);

            if (!filter(dbSchema, tableName)) {
                logger.info("The data of table [{}] have bean aborted", fullName);
                return;
            }

            String schemaName = Utils.buildAvroSchemaName(data.getSchemaHash(), data.getTableName());
            Schema avroSchema = SchemaProvider.getInstance().getSchema(schemaName);

            List<GenericRecord> results = new ArrayList<>(dataList.size());

            for (GenericData gd : dataList) {
                try {
                    results.addAll(GenericSchemaDecoder.decoder().decode(avroSchema, gd.getPayload()));
                } catch (Exception e) {
                    logger.warn("[{}]Decode avro data error. New functional index maybe a reason. Table meta information need to be reload.", fullName);

                    // avro 解析失败，到oracle源端重新获取meta信息
                    MetaVersion v = MetaVerController.getVersionFromCache(dbSchema, tableName);
                    MetaWrapper latestMeta = MetaFetcherManager.getOraMetaFetcher().fetch(dbSchema, tableName, -999);
                    syncMeta(v, latestMeta, offset);
                    logger.info("Meta of {}.{} was reloaded.", dbSchema, tableName);

                    // 删除avro schema缓存
                    SchemaProvider.getInstance().remove(schemaName);
                    logger.info("Cached avro schema is removed.");
                    // 根据重新生成新meta重新生成的avro schema再次解析数据
                    avroSchema = SchemaProvider.getInstance().getSchema(schemaName);
                    logger.info("New avro schema was generated:{}", avroSchema.toString());
                    try {
                        results.addAll(GenericSchemaDecoder.decoder().decode(avroSchema, gd.getPayload()));
                        logger.info("[{}] Decode avro data with new generated avro schema successful.", schemaName);
                    } catch (Exception ex) {
                        sendTermination(v, input, offset);
                        logger.info("Termination message was sent.");
                        logger.error("[{}] Fail to decode avro data with new avro schema.", schemaName, ex);
                        return;
                    }
                }
            }

            long pos = Long.parseLong(results.get(0).get(Constants.MessageBodyKey.POS).toString());

            //TODO 2.获取version以及meta信息
            MetaVersion version = MetaVerController.getSuitableVersion(dbSchema, tableName, pos, offset);
            if (version == null) {
                // topology中的spout、和上级bolt存在table和version是否存在的验证，正常逻辑不会执行到这里
                logger.error("table[{}.{}] or version not found, data was ignored.", schemaName, tableName);
                return;
            }
            MetaWrapper meta = version.getMeta();

            //*********************************************************************
            if (logger.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(version.getTable()).append(":");
                for (MetaWrapper.MetaCell cell : meta.getColumns()) {
                    buf.append(cell.getColumnName()).append(",");
                }
                logger.debug("[fields-order] {} appender version: {}", offset, buf.toString());
            }
            //***********************************************************************

            //TODO 3.将前两步获取到的avro schema 和 meta数据 emit 到下一个bolt处理
            //Schema as = avroSchema;
            DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(version.getSchema(), version.getTable()));

            List<OraWrapperData> wpList = new ArrayList<>();
            for (GenericRecord record : results) {
                OraWrapperData d = new OraWrapperData();
                PairWrapper<String, Object> wrapper = BoltCommandHandlerHelper.convertAvroRecord(record, Constants.MessageBodyKey.noorderKeys);
                d.setDataWrapper(wrapper);
                if (wrapper.getProperties(Constants.MessageBodyKey.OP_TYPE).toString().toLowerCase().equals("u") && table.getOutputBeforeUpdateFlg() != 0) {
                    d.setBeforeDataWrapper(BoltCommandHandlerHelper.convertAvroRecordUseBeforeMap(record, Constants.MessageBodyKey.noorderKeys));
                }
                wpList.add(d);
            }

            EmitData ed = new EmitData();
            //emitData.add(EmitData.AVRO_SCHEMA, as);
            ed.add(EmitData.VERSION, version);
            ed.add(EmitData.OFFSET, offset);
            ed.add(EmitData.MESSAGE, JSON.toJSONString(wpList));
            ed.add(EmitData.DATA_TABLE, table);

            // 测试列顺序使用
            //d.add("fields-order", buf.toString());
            logger.debug("[appender-appender] emit data offset: {}, table: {}", offset, table.getTableName());
            this.emit(listener.getOutputCollector(), input, groupField(dbSchema, tableName), ed, emitCmd);
        } catch (Exception e) {
            logger.error("Error when processing command: {}, data: {}, input: {}", emitCmd.name(), JSON.toJSONString(emitData), input, e);
            throw new RuntimeException(e);
        }
    }

    private boolean filter(String dbSchema, String tableName) {
        DataTable dataTable = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(dbSchema, tableName));
        if (dataTable == null) {
            return false;
        } else if ((dbSchema + "." + tableName).equalsIgnoreCase(PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.HEARTBEAT_SRC))) {
            return true;
        } else {
            return !dataTable.isAbort();
        }
    }

    private void syncMeta(MetaVersion version, MetaWrapper newMeta, long offset) throws Exception {
        version.setMeta(newMeta);
        version.setOffset(offset);
        // 将Version信息和meta信息持久化到mysql
        DBFacadeManager.getDbFacade().createMetaVersion(version);
        logger.info("New version is:{}", version.toString());
    }

    private void sendTermination(MetaVersion ver, Tuple input, long offset) {
        DbusMessage message = BoltCommandHandlerHelper.buildTerminationMessage(ver.getSchema(), ver.getTable(), ver.getVersion());

        EmitData data = new EmitData();
        data.add(EmitData.AVRO_SCHEMA, EmitData.NO_VALUE);
        data.add(EmitData.VERSION, ver);
        data.add(EmitData.MESSAGE, message);
        data.add(EmitData.OFFSET, offset);
        String groupId = groupField(ver.getSchema(), ver.getTable());

        this.emit(listener.getOutputCollector(), input, groupId, data, Command.DATA_INCREMENT_TERMINATION);

        // 修改data table表状态
        BoltCommandHandlerHelper.changeDataTableStatus(ver.getSchema(), ver.getTable(), DataTable.STATUS_ABORT);
    }
}
