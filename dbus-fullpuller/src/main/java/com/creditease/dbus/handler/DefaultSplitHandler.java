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


package com.creditease.dbus.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.FullPullHistory;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.helper.DBHelper;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.manager.GenericSqlManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/10
 */
public class DefaultSplitHandler extends SplitHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void executeSplit() throws Exception {
        GenericSqlManager dbManager = null;
        try {
            JSONObject reqJson = JSONObject.parseObject(reqString);
            dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
            //获取分片列
            int splitShardSize = dbConf.getSplitShardSize();
            String splitByCol = "";
            // 如果splitShardSize 为 -1，不分片
            if (splitShardSize != -1) {
                splitByCol = DBHelper.getSplitColumn(dbManager, dbConf);
                logger.info("[split bolt] doSplitting() Will use col [{}] to split data.", splitByCol);
            } else {
                logger.info("[split bolt] splitShardSize id -1 ,doSplitting() Will not split data ");
            }

            //把分片列,条件更新到全量历史库
            if (StringUtils.isNotBlank(splitByCol) || StringUtils.isNotBlank(dbConf.getInputConditions())) {
                FullPullHistory fullPullHistory = new FullPullHistory();
                fullPullHistory.setId(FullPullHelper.getSeqNo(reqJson));
                fullPullHistory.setSplitColumn(splitByCol);
                fullPullHistory.setFullpullCondition(dbConf.getInputConditions());
                FullPullHelper.updateStatusToFullPullHistoryTable(fullPullHistory);
            }
            //根据分片列获取分片信息
            execute(splitByCol, dbManager);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (dbManager != null) {
                    dbManager.close();
                }
            } catch (Exception e) {
                logger.error("close dbManager error.", e);
            }
        }
    }

    public void execute(String splitCol, GenericSqlManager dbManager) throws Exception {
        JSONObject reqJson = JSON.parseObject(reqString);
        String dsKey = FullPullHelper.getDataSourceKey(reqJson);
        //分片大小
        int splitShardSize = dbConf.getSplitShardSize();
        //构建monitor节点路径
        String progressInfoNodePath = FullPullHelper.getMonitorNodePath(reqString);
        //获取物理表
        String[] physicalTables = dbConf.getString(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY)
                .split(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
        //获取表分区信息
        Map tablePartitionsMap = (Map) dbConf.getConfProperties().get(DBConfiguration.TABEL_PARTITIONS);
        logger.info("[split bolt] Physical Tables count:{}; ", tablePartitionsMap.size());

        boolean hasNotProperSplitCol = StringUtils.isBlank(splitCol);
        long totalRowsCount = 0;
        long totalPartitionCount = 0;
        long splitIndex = 0;
        Long firstShardOffset = null;
        Long lastShardOffset = null;
        RecordMetadata producedRecord = null;

        JSONObject splitWrapper = new JSONObject();
        if (hasNotProperSplitCol) {
            //如果不分片的情况
            for (String table : physicalTables) {
                List<String> tablePartitions = (List<String>) tablePartitionsMap.get(table);
                totalPartitionCount += tablePartitions.size();
                logger.info("[split bolt] {} Partitions count: {}.", table, tablePartitions.size());
                for (String tablePartition : tablePartitions) {
                    long currentTableRows = dbManager.queryTotalRows(table, splitCol, tablePartition);
                    totalRowsCount = totalRowsCount + currentTableRows;

                    DataDBInputSplit inputSplit = new DataDBInputSplit(-1, "1", " = ", "1", " = ", "1");
                    inputSplit.setTargetTableName(table);
                    inputSplit.setTablePartitionInfo(tablePartition);

                    splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                    splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                    splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                    String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                    ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                    Future<RecordMetadata> future = byteProducer.send(record);
                    producedRecord = future.get();
                    splitWrapper.clear();
                    if (splitIndex == 1) {
                        firstShardOffset = producedRecord.offset();
                    }
                    writerSplitStatusToZkAndDB(progressInfoNodePath, totalPartitionCount, splitIndex, totalRowsCount);
                    logger.info("[split bolt] {},生成第{}片分片, 所属分区{}.{} ,当前分区表数据量{}, 累计表数据量{}",
                            dsKey, splitIndex, table, tablePartition, currentTableRows, totalRowsCount);
                }
            }
            lastShardOffset = producedRecord.offset();
        } else {
            String pullCollate = "";
            String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            // 仅mysql需要考虑设置collate
            if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())) {
                pullCollate = FullPullHelper.getConfFromZk(FullPullConstants.COMMON_CONFIG, FullPullConstants.PULL_COLLATE_KEY, false);
                pullCollate = (pullCollate != null) ? pullCollate : "";
            }

            String logicalTableName = dbConf.getInputTableName();
            logger.info("[split bolt] logicalTableName=" + logicalTableName);
            String splitterStyle = (String) dbConf.get(DBConfiguration.SPLIT_STYLE);
            splitterStyle = StringUtils.isNotBlank(splitterStyle) ? splitterStyle : FullPullConstants.SPLITTER_STRING_STYLE_DEFAULT;

            logger.info("[split bolt] splitterStyle=" + splitterStyle);
            if (splitterStyle.equals("md5") || splitterStyle.equals("number")) {
                pullCollate = "";
            }
            logger.info("[split bolt] pullCollate=" + pullCollate);

            for (String table : physicalTables) {
                List<String> tablePartitions = (List<String>) tablePartitionsMap.get(table);
                totalPartitionCount += tablePartitions.size();
                logger.info("[split bolt] {} Partitions count {}.", table, tablePartitions.size());
                for (String tablePartition : tablePartitions) {
                    long currentTableRows = dbManager.queryTotalRows(table, splitCol, tablePartition);
                    long currentTableShard = currentTableRows % splitShardSize == 0
                            ? (currentTableRows / splitShardSize) : (currentTableRows / splitShardSize + 1);
                    totalRowsCount = totalRowsCount + currentTableRows;

                    List<InputSplit> inputSplits = dbManager.querySplits(table, splitCol, tablePartition, splitterStyle, pullCollate, currentTableShard);
                    for (InputSplit inputSplit : inputSplits) {
                        splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                        splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                        splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                        String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                        ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                        Future<RecordMetadata> future = byteProducer.send(record);
                        producedRecord = future.get();
                        splitWrapper.clear();
                        if (splitIndex == 1) {
                            firstShardOffset = producedRecord.offset();
                        }
                        DataDBInputSplit dataDBInputSplit = (DataDBInputSplit) inputSplit;
                        logger.info("[split bolt] {},生成第{}片分片, 所属表分区{}.{} ,当前表分区分片数量{}片, 当前表分区数据量{}条, 累计数据量{}条, 当前分片下界{},上界{}",
                                dsKey, splitIndex, table, tablePartition, currentTableShard, currentTableRows, totalRowsCount,
                                dataDBInputSplit.getLowerValue(), dataDBInputSplit.getUpperValue());

                    }
                    writerSplitStatusToZkAndDB(progressInfoNodePath, totalPartitionCount, splitIndex, totalRowsCount);
                }
            }
            lastShardOffset = producedRecord.offset();
        }
        //向monitor节点写分片信息
        writeSplitResultToZkAndDB(totalPartitionCount, splitIndex, totalRowsCount, firstShardOffset, lastShardOffset);
        logger.info("[split bolt] {} 生成分片完毕，累计分片数量{}片,累计数据量{}条,累计表分区数量{}个", dsKey, splitIndex, totalRowsCount, totalPartitionCount);
    }

}
