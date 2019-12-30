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


package com.creditease.dbus.stream.db2.appender.bolt.processor.appender;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.meta.Db2MetaComparator;
import com.creditease.dbus.commons.meta.MetaComparator;
import com.creditease.dbus.commons.meta.MetaCompareResult;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaEventWarningSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.common.tools.DateUtil;
import com.creditease.dbus.stream.db2.appender.avro.GenericData;
import com.creditease.dbus.stream.db2.appender.avro.GenericSchemaDecoder;
import com.creditease.dbus.stream.db2.appender.bolt.processor.vo.AppenderDataResults;
import com.creditease.dbus.stream.db2.appender.bolt.processor.vo.AppenderDataWrapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;


public class AppendBoltDefaultHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private SchemaRegistryClient schemaRegistry;
    private MetaEventWarningSender sender = new MetaEventWarningSender();

    public AppendBoltDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
        Properties props = null;
        try {
            props = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE);
        } catch (Exception e) {
            logger.error("load properties [schema.registry.rest.url] failed! ERROR: " + e);
        }
        String restUrl = props.getProperty(Constants.ConfigureKey.SCHEMA_REGISTRY_REST_URL);
        schemaRegistry = new CachedSchemaRegistryClient(restUrl, 20, null);
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

            // dataList中存储同一个表的数据
            // 如果是增量拖回重跑的话，可能会导致过来的一批数据中存在不同meta版本的数据
            // 不过这对解析数据没有影响
            GenericData data = dataList.get(0);
            int idx = data.getTableName().indexOf(".");
            String fullName = data.getTableName();
            String dbSchema = fullName.substring(0, idx);
            String tableName = fullName.substring(idx + 1);

            if (!filter(dbSchema, tableName)) {
                logger.info("The data of table [{}] have bean aborted", fullName);
                return;
            }

            Map<String, AppenderDataWrapper> versionData = new TreeMap<>();

            List<AppenderDataResults> results = new ArrayList<>();
            MetaVersion v = null;
            //解析每条数据
            for (GenericData gd : dataList) {

                Schema schema;
                ByteBuffer buffer = ByteBuffer.wrap(gd.getPayload());

                if (buffer.get() != Constants.MAGIC_BYTE) {
                    logger.error("Unknown magic byte!");
                }

                int id = buffer.getInt();

                schema = ThreadLocalCache.get(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(dbSchema, tableName, String.valueOf(id)));
                if (schema == null) {
                    schema = schemaRegistry.getById(id);
                    ThreadLocalCache.put(Constants.CacheNames.TABLE_SCHEMA_VERSION_CACHE, Utils.buildDataTableSchemaCacheKey(dbSchema, tableName, String.valueOf(id)), schema);
                }

                int length = buffer.limit() - 5;
                int start = buffer.position() + buffer.arrayOffset();
                List<GenericRecord> records = GenericSchemaDecoder.decoder().decode(schema, gd.getPayload(), start, length);
                Long time = DateUtil.convertStrToLong4Date(records.get(0).get(Constants.DB2MessageBodyKey.DB2_CURTMSTP).toString(), Constants.DB2MessageBodyKey.DB2_CURTMSTP_FORMAT);
                String strId = String.valueOf(id);

                logger.info("record info[time: {}, pos: {}]", time, strId);

                //先从cache中获取meta version，cache中没有，则去数据库查找
                v = MetaVerController.getMetaVersionFromCache(dbSchema, tableName, strId, time);
                if (v == null)
                    v = MetaVerController.getMetaVersionFromDB(dbSchema, tableName, strId, time);
                if (v != null) {
                    if (versionData.containsKey(strId)) {
                        MetaVersion ver = versionData.get(strId).getVersion();
                        //相同的schema id，但是版本号不同，将之前versionData中该schema id的数据发送出去
                        if (ver.getVersion() != v.getVersion()) {
                            emitSameVersionData(input, emitCmd, offset, dbSchema, tableName, versionData.get(strId).getResults(), ver);
                            versionData.remove(strId);
                            AppenderDataWrapper adw = versionData.get(strId);
                            if (adw == null) adw = new AppenderDataWrapper();
                            results = adw.getResults();
                            AppenderDataResults appDataResults = new AppenderDataResults();
                            appDataResults.setGenericRecord(records.get(0));
                            appDataResults.setOffset(gd.getOffset());
                            results.add(appDataResults);
                            adw.setResults(results);
                            adw.setVersion(v);
                            versionData.put(strId, adw);
                        } else {
                            AppenderDataWrapper adw = versionData.get(strId);
                            if (adw == null) adw = new AppenderDataWrapper();
                            results = adw.getResults();
                            AppenderDataResults appDataResults = new AppenderDataResults();
                            appDataResults.setGenericRecord(records.get(0));
                            appDataResults.setOffset(gd.getOffset());
                            results.add(appDataResults);
                            adw.setResults(results);
                            versionData.put(strId, adw);
                        }
                    } else {
                        AppenderDataWrapper adw = new AppenderDataWrapper();
                        adw.setVersion(v);
                        results = adw.getResults();
                        AppenderDataResults appDataResults = new AppenderDataResults();
                        appDataResults.setGenericRecord(records.get(0));
                        appDataResults.setOffset(gd.getOffset());
                        results.add(appDataResults);
                        adw.setResults(results);
                        versionData.put(strId, adw);
                    }
                }
                //如果数据库中也没有该schema id的信息，则说明发生了表结构变更，同步meta信息，同时将该schema id的版本信息存入cache
                else {
                    MetaWrapper latestMeta = MetaFetcherManager.getDb2MetaFetcher().fetch(dbSchema, tableName, -999);
                    v = MetaVerController.getLatestMetaVersionFromDB(dbSchema, tableName);
                    if (v.getVersion() == 0 && v.getTrailPos() == 0) {
                        v.setTrailPos(id);
                    } else {
                        v.setTrailPos(id);
                        v.setVersion(v.getVersion() + 1);
                    }

                    v = syncMeta(v, latestMeta, time);

                    if (versionData.containsKey(strId)) {
                        logger.error("should be not here!");
                    } else {
                        AppenderDataWrapper adw = new AppenderDataWrapper();
                        adw.setVersion(v);
                        results = adw.getResults();
                        AppenderDataResults appDataResults = new AppenderDataResults();
                        appDataResults.setGenericRecord(records.get(0));
                        appDataResults.setOffset(gd.getOffset());
                        results.add(appDataResults);
                        adw.setResults(results);

                        versionData.put(strId, adw);
                    }
                }

            }

//            List<Map.Entry<String,AppenderDataWrapper>> lists = new ArrayList<>(versionData.entrySet());
//            Collections.sort(lists,(o1,o2) -> o1.compareTo(o2));

            //将各个版本的数据发送给下一级bolt处理
            for (Map.Entry<String, AppenderDataWrapper> entry : versionData.entrySet()) {
                emitSameVersionData(input, emitCmd, offset, dbSchema, tableName, entry.getValue().getResults(), entry.getValue().getVersion());
            }

            versionData.clear();

        } catch (Exception e) {
            logger.error("Error when processing command: {}, data: {}, input: {}", emitCmd.name(), JSON.toJSONString(emitData), input, e);
            throw new RuntimeException(e);
        }
    }

    private void emitSameVersionData(Tuple input, Command emitCmd, long offset, String dbSchema, String tableName, List<AppenderDataResults> results, MetaVersion v) {
        //同一批数据可能包含不止一个版本的数据，对于同一个版本v的数据，直接emit给下一级bolt处理
        DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(v.getSchema(), v.getTable()));

        EmitData ed = new EmitData();

        ed.add(EmitData.VERSION, v);
        ed.add(EmitData.OFFSET, offset);
        ed.add(EmitData.MESSAGE, results);
        ed.add(EmitData.DATA_TABLE, table);

        logger.info("emit same version data, table: {}, offset: {}, record count: {}, version id: {}", tableName, offset, results.size(), v.getVersion());
        this.emit(listener.getOutputCollector(), input, groupField(dbSchema, tableName), ed, emitCmd);
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

    private MetaVersion syncMeta(MetaVersion version, MetaWrapper newMeta, long time) throws Exception {

        MetaComparator comparator = new Db2MetaComparator();
        MetaCompareResult result = comparator.compare(version.getMeta(), newMeta);
        sendMetaEventWarningMessage(version, newMeta, result);

        version.setMeta(newMeta);
        version.setOffset(time);
        // 将version信息和meta信息持久化到mysql
        DBFacadeManager.getDbFacade().createMetaVersion(version);
        logger.info("Meta version is changed! New version is:{}", version.toString());

        return MetaVerController.putMetaVersionCache(version);
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

    private void sendMetaEventWarningMessage(MetaVersion ver, MetaWrapper newMeta, MetaCompareResult result) {
        logger.info("send meta version changed message. version: {}", ver.getVersion());
        sender.sendMessage(ver, newMeta, result);
    }
}
