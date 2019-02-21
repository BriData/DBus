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

package com.creditease.dbus.stream.mysql.appender.bolt.processor.appender;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.meta.MetaComparator;
import com.creditease.dbus.commons.meta.MetaCompareResult;
import com.creditease.dbus.commons.meta.MysqlMetaComparator;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaEventWarningSender;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherManager;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.creditease.dbus.stream.mysql.appender.protobuf.Support;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.EntryHeader;
import com.creditease.dbus.stream.mysql.appender.protobuf.protocol.MessageEntry;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;

public class MaDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;
    private MetaEventWarningSender sender;

    public MaDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
        this.sender = new MetaEventWarningSender();
    }

    @Override
    public void handle(Tuple input) {
        EmitData emitData = (EmitData) input.getValueByField(Constants.EmitFields.DATA);
        Command emitCmd = (Command) input.getValueByField(Constants.EmitFields.COMMAND);
        List<MessageEntry> dataList = emitData.get(EmitData.MESSAGE);

        try {
            long offset = emitData.get(EmitData.OFFSET);
            logger.debug("[BEGIN] receive data,offset:{}", offset);

            if (dataList == null || dataList.isEmpty()) {
                return;
            }
            MessageEntry msgEntry = dataList.get(0);
            String dbSchema = msgEntry.getEntryHeader().getSchemaName();
            String tableName = msgEntry.getEntryHeader().getTableName();

            if (!filter(dbSchema, tableName)) {
                logger.info("The data of table [{}.{}] have bean aborted", dbSchema, tableName);
                return;
            }

            processChangedMeta(msgEntry, input, offset);

            //if (processChangedMeta(msgEntry, input, offset)) {
            //    return;
            //}

            long pos = Long.parseLong(msgEntry.getEntryHeader().getPos());

            //TODO 2.获取version以及meta信息
            MetaVersion version = MetaVerController.getSuitableVersion(dbSchema, tableName, pos, offset);
            if (version == null) {
                // topology中的spout、和上级bolt存在table和version是否存在的验证，正常逻辑不会执行到这里
                logger.error("table[{}.{}] or version not found, data was ignored.", dbSchema, tableName);
                return;
            }
            DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(version.getSchema(), version.getTable()));

            //TODO 3.将前两步获取到的avro schema 和 meta数据 emit 到下一个bolt处理
            emitData = new EmitData();
            emitData.add(EmitData.VERSION, version);
            emitData.add(EmitData.OFFSET, offset);
            emitData.add(EmitData.MESSAGE, dataList);
            emitData.add(EmitData.DATA_TABLE, table);
            this.emit(listener.getOutputCollector(), input, groupField(dbSchema, tableName), emitData, emitCmd);
        } catch (Exception e) {
            logger.error("Error when processing command: {}, input: {}", emitCmd.name(), input, e);
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

    private boolean processChangedMeta(MessageEntry msgEntry, Tuple input, long offset) throws Exception {
        boolean bRet = false;
        EntryHeader header = msgEntry.getEntryHeader();
        long pos = Long.parseLong(msgEntry.getEntryHeader().getPos());
        MetaVersion version = MetaVerController.getVersionFromCache(header.getSchemaName(), header.getTableName());
        String key = StringUtils.join(new String[]{header.getSchemaName(), header.getTableName()}, ".");
        DataTable dataTable = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key);
        if (version != null && version.getTrailPos() < pos && dataTable.isMetaChanged()) {
            MetaWrapper metaWrapper = buildMeta(msgEntry);

            MetaComparator comparator = new MysqlMetaComparator();
            MetaCompareResult result = comparator.compare(version.getMeta(), metaWrapper);


            /**
             * meta信息不兼容不升版本，只需要删除重建t_table_meta中的meta信息
             * 兼容则只需要清除metaChangeFlag
             */

            if (version.getMeta() != null && !result.isCompatible()) {
                // 发送termination消息
                logger.info("Meta is not compatible");
                version.setMeta(metaWrapper);

                // 将Version信息和meta信息持久化到mysql
                DBFacadeManager.getDbFacade().deleteAndInsertTableMeta(version);
            }

            BoltCommandHandlerHelper.clearMetaChangeFlag(header.getSchemaName(), header.getTableName());
            logger.info("New version is:{}", version.toString());

        } else if (version == null) {
            MetaVersion metaVer = new MetaVersion();
            MetaWrapper metaWrapper = buildMeta(msgEntry);
            metaVer.setSchema(header.getSchemaName());
            metaVer.setTable(header.getTableName());
            metaVer.setVersion(0);
            metaVer.setInnerVersion(0);
            metaVer.setMeta(metaWrapper);
            metaVer.setOffset(offset);
            metaVer.setTrailPos(Long.parseLong(msgEntry.getEntryHeader().getPos()));
            metaVer.setTableId(dataTable.getId());
            metaVer.setDsId(dataTable.getDsId());
            MetaVerController.putVersion(key, metaVer);

            DBFacadeManager.getDbFacade().createMetaVersion(metaVer);
            BoltCommandHandlerHelper.clearMetaChangeFlag(header.getSchemaName(), header.getTableName());
        } else if (version.getMeta() == null || version.getMeta().isEmpty()) {
            MetaWrapper meta = buildMeta(msgEntry);
            version.setMeta(meta);
            version.setOffset(offset);
            version.setTrailPos(Long.parseLong(msgEntry.getEntryHeader().getPos()));
            DBFacadeManager.getDbFacade().saveMeta(version.getId(), meta);

            BoltCommandHandlerHelper.clearMetaChangeFlag(header.getSchemaName(), header.getTableName());
        }
        return bRet;
    }

    private MetaWrapper buildMeta(MessageEntry msgEntry) throws Exception {
        MetaWrapper metaWrapper = new MetaWrapper();
        EntryHeader header = msgEntry.getEntryHeader();
        RowData rowData = msgEntry.getMsgColumn().getRowDataLst().get(0);
        List<Column> columns = Support.getFinalColumns(header.getOperType(), rowData);
        for (Column column : columns) {
            MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();
            cell.setColumnName(column.getName());
            cell.setOriginalColumnName(cell.getColumnName());
            cell.setDataType(Support.getColumnType(column));
            long[] ret = Support.getColumnLengthAndPrecision(column);
            cell.setDataLength(ret[0]);
            cell.setDataPrecision((int) ret[1]);
            cell.setDataScale(0);
            cell.setIsPk(column.getIsKey() ? "Y" : "N");
            cell.setNullAble("N");
            cell.setDdlTime(new Timestamp(header.getExecuteTime()));
            cell.setColumnId(column.getIndex());
            cell.setInternalColumnId(column.getIndex());
            cell.setHiddenColumn("NO");
            cell.setVirtualColumn("NO");
            metaWrapper.addMetaCell(cell);
        }

        /**
         * 从源端获取列的注释信息，这里列信息有可能不一致
         * 只赋值一致的部分，不一致的部分没办法处理
         * 即需要将数据生成的metaWrapper中的注释填充为sourceMetaWrapper的注释
         */
        MetaWrapper sourceMetaWrapper = MetaFetcherManager.getMysqlMetaFetcher().fetch(header.getSchemaName(), header.getTableName(), -9999);
        for (MetaWrapper.MetaCell cell : metaWrapper.getColumns()) {
            String columnName = cell.getColumnName();
            MetaWrapper.MetaCell sourceCell = sourceMetaWrapper.get(columnName);
            // 可能源端已经不存在该列了
            if (sourceCell == null) continue;
            // TODO: 2017/9/22
            /**
             * dataType和precision如何设置
             */
            cell.setDataLength(sourceCell.getDataLength());
            cell.setDataPrecision(sourceCell.getDataPrecision());
            cell.setDataScale(sourceCell.getDataScale());
            cell.setNullAble(sourceCell.getNullAble());
            cell.setComments(sourceCell.getComments());
        }

        return metaWrapper;
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

    @SuppressWarnings("unused")
    private void sendMetaEventWarningMessage(MetaVersion ver, MetaWrapper newMeta, MetaCompareResult result) {
        sender.sendMessage(ver, newMeta, result);
    }
}
