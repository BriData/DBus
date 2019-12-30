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


package com.creditease.dbus.stream.mysql.appender.bolt.processor.appender;

import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MaasAppenderMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.meta.MetaComparator;
import com.creditease.dbus.commons.meta.MetaCompareResult;
import com.creditease.dbus.commons.meta.MysqlMetaComparator;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DdlEvent;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bean.TableComments;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaEventWarningSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 处理mysql 同步meta
 *
 * @author xiongmao
 */
public class MaMetaSyncEventHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private MetaEventWarningSender sender;

    public MaMetaSyncEventHandler(CommandHandlerListener listener) {
        this.listener = listener;
        this.sender = new MetaEventWarningSender();
    }

    @Override
    public void handle(Tuple tuple) {
        try {
            EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
            List<MessageEntry> datas = emitData.get(EmitData.MESSAGE);
            long offset = emitData.get(EmitData.OFFSET);

            logger.debug("[BEGIN] receive data,offset:{}", offset);

            for (MessageEntry msgEntry : datas) {
                long pos = Long.parseLong(msgEntry.getEntryHeader().getPos());
                syncMeta(msgEntry, pos, offset, tuple);
            }
        } catch (Exception e) {
            logger.error("Error when processing data", e);
            throw new RuntimeException(e);
        }
    }

    private void syncMeta(MessageEntry msgEntry, long pos, long offset, Tuple input) throws Exception {
        logger.info("Received a meta sync message {}", msgEntry.getSql());
        //处理表结构变化消息,包含truncate和alter
        //	String ddlType = StringUtils.substringBefore(msgEntry.getSql(), " ").toUpperCase();
        EntryHeader header = msgEntry.getEntryHeader();
        if (header.isTruncate()) {
            MetaVersion version = MetaVerController.getSuitableVersion(header.getSchemaName(), header.getTableName(), pos, offset);
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", header.getSchemaName(), header.getTableName());
                return;
            }

            // 生成版本号之前发送termination消息
            //sendTermination(version);

            // truncate处理为meta namespace变更,wormhole团队可以容易处理
            // 改变外部版本号,外部版本号变化后wormhole会根据版本号分区相同表的数据
            version.nextVer();
            MetaVerController.updateVersion(version);

            logger.info(String.format("Message version changed[schema:%s, table:%s, type:%s]", header.getSchemaName(), header.getTableName(), header.getOperType().name()));

            DBFacadeManager.getDbFacade().insertDdlEvent(generateDdlEvent(version, msgEntry));

        } else if (header.isAlter()) {
            MetaVersion version = MetaVerController.getVersionFromCache(header.getSchemaName(), header.getTableName());
            if (version == null) {
                logger.warn("The version of table {}.{} was not found.", header.getSchemaName(), header.getTableName());
                return;
            }

            MetaWrapper metaWrapper = MetaFetcherManager.getMysqlMetaFetcher().fetch(header.getSchemaName(), header.getTableName(), -9999);

            MetaComparator comparator = new MysqlMetaComparator();
            MetaCompareResult result = comparator.compare(version.getMeta(), metaWrapper);

            // meta信息不兼容则升版本
            if (version.getMeta() != null && !result.isCompatible()) {
                // 发送termination消息
                sendTermination(version, input);
                version.nextVer();
                logger.info("Meta is not compatible");
            }
            sendMetaEventWarningMessage(version, metaWrapper, result);
            version.nextInnerVer();
            version.setMeta(metaWrapper);
            version.setOffset(offset);
            version.setTrailPos(Long.parseLong(msgEntry.getEntryHeader().getPos()));

            // 获取表的注释信息
            TableComments tableComments = MetaFetcherManager.getMysqlMetaFetcher().fetchTableComments(header.getSchemaName(), header.getTableName());

            version.setComments(tableComments.getComments());
            // 将Version信息和meta信息持久化到mysql
            DBFacadeManager.getDbFacade().createMetaVersion(version);

            BoltCommandHandlerHelper.setMetaChangeFlag(header.getSchemaName(), header.getTableName());
            logger.info("New version is:{}", version.toString());

            DBFacadeManager.getDbFacade().insertDdlEvent(generateDdlEvent(version, msgEntry));

            // 写kafka增量事件，给心跳使用
            sendMaasAppenderMessageAlter(version, msgEntry.getSql());

        } else {


        }
    }

    private DdlEvent generateDdlEvent(MetaVersion version, MessageEntry msgEntry) {

        EntryHeader entryHeader = msgEntry.getEntryHeader();

        DdlEvent ddlEvent = new DdlEvent();

        ddlEvent.setDsId(Utils.getDatasource().getId());
        ddlEvent.setSchemaName(entryHeader.getSchemaName());
        ddlEvent.setTableName(entryHeader.getTableName());

        ddlEvent.setVerId(version.getId());
        ddlEvent.setDdlType(entryHeader.getOperType().toString());
        ddlEvent.setDdl(msgEntry.getSql());

        return ddlEvent;
    }

    private void sendTermination(MetaVersion ver, Tuple input) {
        DbusMessage message = BoltCommandHandlerHelper.buildTerminationMessage(ver.getSchema(), ver.getTable(), ver.getVersion());

        EmitData data = new EmitData();
        data.add(EmitData.AVRO_SCHEMA, EmitData.NO_VALUE);
        data.add(EmitData.VERSION, ver);
        data.add(EmitData.MESSAGE, message);

        this.emit(listener.getOutputCollector(), input, groupField(ver.getSchema(), ver.getTable()), data, Command.DATA_INCREMENT_TERMINATION);

        // 修改data table表状态
        //BoltCommandHandlerHelper.changeDataTableStatus(ver.getSchema(), ver.getTable(), DataTable.STATUS_ABORT);
    }

    private void sendMetaEventWarningMessage(MetaVersion ver, MetaWrapper newMeta, MetaCompareResult result) {
        sender.sendMessage(ver, newMeta, result);
    }

    private void sendMaasAppenderMessageAlter(MetaVersion ver, String sql) {
        MaasAppenderMessage maasAppenderMessage = new MaasAppenderMessage();
        maasAppenderMessage.setDsId(GlobalCache.getDatasource().getId());
        maasAppenderMessage.setDsType(GlobalCache.getDatasource().getDsType());
        maasAppenderMessage.setSchemaName(ver.getSchema());
        maasAppenderMessage.setTableName(ver.getTable());
        maasAppenderMessage.setTableComment(ver.getComments());
        maasAppenderMessage.setType(MaasAppenderMessage.Type.ALTER);
        maasAppenderMessage.setDdl(sql);
        sender.sendMaasAppenderMessage(maasAppenderMessage);
    }

    public static void main(String[] args) {
        Date date = new Date();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        System.out.println(format.format(date));

        Timestamp ts = new Timestamp(1473755543000L);
        System.out.println(ts.toString());
        date = ts;
        System.out.println(format.format(date));
    }
}
