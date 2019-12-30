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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MaasAppenderMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.meta.MetaComparator;
import com.creditease.dbus.commons.meta.MetaCompareResult;
import com.creditease.dbus.commons.meta.OraMetaComparator;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.*;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaEventWarningSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherException;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 处理同步meta事件
 * Created by Shrimp on 16/7/1.
 */
public class MetaSyncEventHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private MetaEventWarningSender sender;

    /**
     * meta_sync_event表的ddl_type存储的值
     */
    private static final String DDL_TYPE_TRUNCATE = "TRUNCATE";
    private static final String DDL_TYPE_ALTER = "ALTER";
    private static final String DDL_TYPE_COMMENT_COLUMN = "COMMENT-COLUMN";
    private static final String DDL_TYPE_COMMENT_TABLE = "COMMENT-TABLE";

    private String sourceVersion;

    private boolean is2X() {
        return StringUtils.equals(sourceVersion, OLD_2X_SOURCE_VERSION);
    }

    private boolean is3X() {
        return StringUtils.equals(sourceVersion, NEW_3X_SOURCE_VERSION);
    }

    private String getSourceVersion(MetaSyncEvent e) {
        JSONObject json;
        // 2.X版本去解析json一定失败
        try {
            json = JSON.parseObject(e.getDdl());
        } catch (Exception exception) {
//            logger.warn("parse ddl to json failed, source is 2.x", exception);
            return OLD_2X_SOURCE_VERSION;
        }

        // 目前3.X版本trigger的DDL中version是"1"
        if (StringUtils.equals(json.getString("version"), "1")) {
            return NEW_3X_SOURCE_VERSION;
        }
        return UNKNOWN_SOURCE_VERSION;
    }

    private static final String OLD_2X_SOURCE_VERSION = "OLD_2X_SOURCE_VERSION";
    private static final String NEW_3X_SOURCE_VERSION = "NEW_3X_SOURCE_VERSION";
    private static final String UNKNOWN_SOURCE_VERSION = "UNKNOWN_SOURCE_VERSION";

    public MetaSyncEventHandler(CommandHandlerListener listener) {
        this.listener = listener;
        this.sender = new MetaEventWarningSender();
    }

    @Override
    public void handle(Tuple tuple) {
        try {
            EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
            List<PairWrapper<String, Object>> wrapperList = emitData.get(EmitData.MESSAGE);
            long offset = emitData.get(EmitData.OFFSET);

            logger.debug("[BEGIN] receive data,offset:{}", offset);

            for (PairWrapper<String, Object> wrapper : wrapperList) {
                long pos = Long.parseLong(wrapper.getProperties(Constants.MessageBodyKey.POS).toString());
                Map<String, Object> map = wrapper.pairs2map();
                try {
                    syncMeta(MetaSyncEvent.build(map), pos, offset, tuple);
                } catch (MetaFetcherException e) {
                    String strategy = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.META_FETCHER_EXCEPTION_STRATEGY);
                    if ("skip".equals(strategy)) {
                        logger.warn("meta fetcher error was skipped.", e.getMessage(), e);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error("Error when processing data", sw.toString());
            logger.error("Error when processing data", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 同步meta信息
     *
     * @param e MetaSyncEvent
     */
    private void syncMeta(MetaSyncEvent e, long pos, long offset, Tuple input) throws Exception {

        DataTable dataTable = DBFacadeManager.getDbFacade().queryDataTable(Utils.getDatasource().getId(), e.getSchema(), e.getTableName());
        if (dataTable == null) {
            logger.warn("table[{}.{}] not exists.", e.getSchema(), e.getTableName());
            return;
        } else if (dataTable.isAbort()) {
            logger.warn("table[{}.{}] is aborted.", e.getSchema(), e.getTableName());
            return;
        }
        sourceVersion = getSourceVersion(e);

        if (StringUtils.equals(sourceVersion, UNKNOWN_SOURCE_VERSION)) {
            return;
        }

        logger.info(String.format("Received a meta sync message[%s]", e.toString()));

        // 需要检测是否已经出现在了ddl_event表中
        // 如果有,那么不需要再处理该事件

        //处理表结构变化消息,包含truncate和alter
        if (DDL_TYPE_TRUNCATE.equals(e.getDdlType())) {
            MetaVersion version = MetaVerController.getVersionFromCache(e.getSchema(), e.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", e.getSchema(), e.getTableName());
                return;
            }
            DdlEvent ddlEvent = generateDdlEvent(version, e);
            if (DBFacadeManager.getDbFacade().checkDdlEvent(ddlEvent)) {
                logger.info("已经存在ddlEvent:{}", JSON.toJSONString(ddlEvent));
                return;
            }
            // 生成版本号之前发送termination消息
            //sendTermination(version);

            // truncate处理为meta namespace变更,wormhole团队可以容易处理
            // 外部版本号变化后,wormhole会根据版本号,区分相同表的数据
            version.nextVer();
            MetaVerController.updateVersion(version);

            logger.info(String.format("Message version changed[schema:%s, table:%s, type:%s]", e.getSchema(), e.getTableName(), e.getDdlType()));

            DBFacadeManager.getDbFacade().insertDdlEvent(ddlEvent);

        } else if (DDL_TYPE_ALTER.equals(e.getDdlType())) {
            // 这里不能使用getSuitableVersion()
            MetaVersion version = MetaVerController.getVersionFromCache(e.getSchema(), e.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", e.getSchema(), e.getTableName());
                return;
            }
            DdlEvent ddlEvent = generateDdlEvent(version, e);
            if (DBFacadeManager.getDbFacade().checkDdlEvent(ddlEvent)) {
                logger.info("已经存在ddlEvent:{}", JSON.toJSONString(ddlEvent));
                return;
            }
            // 比较版本,同步meta信息
            // 判断指定版本是否已经同步

            // 查询源数据库获取指定版本的meta信息,并持久化,生成下一版本的meta信息
            // trigger生成的meta信息为ddl发生之前的meta数据,因此取版本 n 的元数据需要使用 n+1 为参数

            MetaWrapper newMeta = null;
            if (is3X()) {
                newMeta = MetaFetcherManager.getOraMetaFetcher().fetch(e.getSchema(), e.getTableName(), e.getVersion() + 1);
            } else if (is2X()) {
                newMeta = MetaFetcherManager.getOraMetaFetcher().fetch2X(e.getSchema(), e.getTableName(), e.getVersion() + 1);
            }

            // 清除掉所有和最新schema相同哈希的版本
            // 如果后续出现了新版本和老版本的哈希值相同(即版本兼容),则删除老版本
            // 首先将哈希值设置为空,之后如果删除了旧版本,就必须将旧版本的哈希值填入此版本
            version.setSchemaHash(null);
            List<MetaVersion> oldVersions = DBFacadeManager.getDbFacade().queryAllMetaVersion(version.getTableId());
            MetaComparator comparator = new OraMetaComparator();
            logger.info("根据兼容性判断是否需要删除旧版本");
            List<Long> oldVersionIds = new ArrayList<>();
            for (MetaVersion old : oldVersions) {
                MetaCompareResult result = comparator.compare(old.getMeta(), newMeta);
                if (result.isCompatible()) {
                    version.setSchemaHash(old.getSchemaHash());
                    oldVersionIds.add(old.getId());
//                    DBFacadeManager.getDbFacade().markVersionDeleted(old.getId());
                }
            }

            MetaCompareResult result = comparator.compare(version.getMeta(), newMeta);

            // meta信息不兼容则升版本
            if (!result.isCompatible()) {
                // 发送termination消息
                sendTermination(version, input, offset);
                // 不兼容的情况下升级外部版本号
                version.nextVer();
                logger.info("Meta is not compatible");
            } else {
                logger.info("meta changed, but is compatible.");
            }
            sendMetaEventWarningMessage(version, newMeta, result);

            version.nextInnerVer();
            version.setMeta(newMeta);
            version.setOffset(offset);
            version.setTrailPos(pos);

            // 获取表的注释信息
            TableComments tableComments = MetaFetcherManager.getOraMetaFetcher().fetchTableComments(e.getSchema(), e.getTableName());
            version.setComments(tableComments.getComments());


            DBFacadeManager.getDbFacade().updateVersionInfo(version, oldVersionIds, ddlEvent);
            // 将Version信息和meta信息持久化到mysql
//            DBFacadeManager.getDbFacade().createMetaVersion(version);
            logger.info("New version is:{}", version.toString());

            // 这里需要重新赋值版本的id,因为已经创建了新版本了
//            ddlEvent.setVerId(version.getId());
//            DBFacadeManager.getDbFacade().insertDdlEvent(ddlEvent);
            // 写kafka增量事件，给心跳使用
            if (is3X()) {
                sendMaasAppenderMessageAlter(version, ddlEvent.getDdl());
            }

        } else if (DDL_TYPE_COMMENT_TABLE.equals(e.getDdlType())) {
            MetaVersion version = MetaVerController.getVersionFromCache(e.getSchema(), e.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", e.getSchema(), e.getTableName());
                return;
            }
            DdlEvent ddlEvent = generateDdlEvent(version, e);
            if (DBFacadeManager.getDbFacade().checkDdlEvent(ddlEvent)) {
                logger.info("已经存在ddlEvent:{}", JSON.toJSONString(ddlEvent));
                return;
            }

            TableComments tableComments = MetaFetcherManager.getOraMetaFetcher().fetchTableComments(e.getSchema(), e.getTableName());

            version.setComments(tableComments.getComments());
            DBFacadeManager.getDbFacade().updateTableComments(version, tableComments);
            logger.info("receive COMMENT-TABLE DDL_TYPE, schema: {}, table: {}, new comments: {}", e.getSchema(), e.getTableName(), tableComments.getComments());

            DBFacadeManager.getDbFacade().insertDdlEvent(ddlEvent);
            sendMaasAppenderMessageCommentTable(version, ddlEvent.getDdl());
        } else if (DDL_TYPE_COMMENT_COLUMN.equals(e.getDdlType())) {
            MetaVersion version = MetaVerController.getVersionFromCache(e.getSchema(), e.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", e.getSchema(), e.getTableName());
                return;
            }
            DdlEvent ddlEvent = generateDdlEvent(version, e);
            if (DBFacadeManager.getDbFacade().checkDdlEvent(ddlEvent)) {
                logger.info("已经存在ddlEvent:{}", JSON.toJSONString(ddlEvent));
                return;
            }
            JSONObject json = JSON.parseObject(e.getDdl());

            String columnName = json.getString("target").split("\\.")[1];
            MetaWrapper metaWrapper = MetaFetcherManager.getOraMetaFetcher().fetch(e.getSchema(), e.getTableName(), e.getVersion() + 1);
            MetaWrapper.MetaCell cell = metaWrapper.get(columnName);

            version.getMeta().get(columnName).setComments(cell.getComments());
            DBFacadeManager.getDbFacade().updateColumnComments(version, cell);
            logger.info("schema: {}, table: {}, column: {}, new comments: {}", e.getSchema(), e.getTableName(), cell.getColumnName(), cell.getComments());

            DBFacadeManager.getDbFacade().insertDdlEvent(ddlEvent);
            sendMaasAppenderMessageCommentColumn(version, cell, ddlEvent.getDdl());
        }
    }


    private DdlEvent generateDdlEvent(MetaVersion version, MetaSyncEvent e) {
        DdlEvent ddlEvent = new DdlEvent();
        ddlEvent.setEventId(e.getSerno());
        ddlEvent.setDsId(version.getDsId());
        ddlEvent.setSchemaName(e.getSchema());
        ddlEvent.setTableName(e.getTableName());

        if (is3X()) {
            JSONObject json = JSON.parseObject(e.getDdl());
            if (DDL_TYPE_COMMENT_COLUMN.equals(e.getDdlType())) {
                ddlEvent.setColumnName(json.getString("target").split("\\.")[1]);
            }
            ddlEvent.setTriggerVer(json.getString("version"));
            ddlEvent.setDdl(new String(Base64.getDecoder().decode(json.getString("ddl"))));
        } else if (is2X()) {
            ddlEvent.setDdl(e.getDdl());
        }

        ddlEvent.setVerId(version.getId());
        ddlEvent.setDdlType(e.getDdlType());

        return ddlEvent;
    }

    private void sendTermination(MetaVersion ver, Tuple input, long offset) {
        DbusMessage message = BoltCommandHandlerHelper.buildTerminationMessage(ver.getSchema(), ver.getTable(), ver.getVersion());

        EmitData data = new EmitData();
        data.add(EmitData.AVRO_SCHEMA, EmitData.NO_VALUE);
        data.add(EmitData.VERSION, ver);
        data.add(EmitData.MESSAGE, message);
        data.add(EmitData.OFFSET, offset);

        this.emit(listener.getOutputCollector(), input, groupField(ver.getSchema(), ver.getTable()), data, Command.DATA_INCREMENT_TERMINATION);

        // 修改data table表状态
        //BoltCommandHandlerHelper.changeDataTableStatus(ver.getSchema(), ver.getTable(), DataTable.STATUS_ABORT);
    }

    private void sendMetaEventWarningMessage(MetaVersion ver, MetaWrapper newMeta, MetaCompareResult result) {
        sender.sendMessage(ver, newMeta, result);
    }

    private void sendMaasAppenderMessageAlter(MetaVersion ver, String ddl) {
        MaasAppenderMessage maasAppenderMessage = new MaasAppenderMessage();
        maasAppenderMessage.setDsId(GlobalCache.getDatasource().getId());
        maasAppenderMessage.setDsType(GlobalCache.getDatasource().getDsType());
        maasAppenderMessage.setSchemaName(ver.getSchema());
        maasAppenderMessage.setTableName(ver.getTable());
        maasAppenderMessage.setTableComment(ver.getComments());
        maasAppenderMessage.setType(MaasAppenderMessage.Type.ALTER);
        maasAppenderMessage.setDdl(ddl);
        sender.sendMaasAppenderMessage(maasAppenderMessage);
    }

    private void sendMaasAppenderMessageCommentTable(MetaVersion ver, String ddl) {
        MaasAppenderMessage maasAppenderMessage = new MaasAppenderMessage();
        maasAppenderMessage.setDsId(GlobalCache.getDatasource().getId());
        maasAppenderMessage.setDsType(GlobalCache.getDatasource().getDsType());
        maasAppenderMessage.setSchemaName(ver.getSchema());
        maasAppenderMessage.setTableName(ver.getTable());
        maasAppenderMessage.setTableComment(ver.getComments());
        maasAppenderMessage.setType(MaasAppenderMessage.Type.COMMENT_TABLE);
        maasAppenderMessage.setDdl(ddl);
        sender.sendMaasAppenderMessage(maasAppenderMessage);
    }

    private void sendMaasAppenderMessageCommentColumn(MetaVersion ver, MetaWrapper.MetaCell cell, String ddl) {
        MaasAppenderMessage maasAppenderMessage = new MaasAppenderMessage();
        maasAppenderMessage.setDsId(GlobalCache.getDatasource().getId());
        maasAppenderMessage.setDsType(GlobalCache.getDatasource().getDsType());
        maasAppenderMessage.setSchemaName(ver.getSchema());
        maasAppenderMessage.setTableName(ver.getTable());
        maasAppenderMessage.setTableComment(ver.getComments());
        maasAppenderMessage.setColumnName(cell.getColumnName());
        maasAppenderMessage.setColumnType(cell.getDataType());
        maasAppenderMessage.setColumnNullable(cell.getNullAble());
        maasAppenderMessage.setColumnComment(cell.getComments());
        maasAppenderMessage.setType(MaasAppenderMessage.Type.COMMENT_COLUMN);
        maasAppenderMessage.setDdl(ddl);
        sender.sendMaasAppenderMessage(maasAppenderMessage);
    }
}
