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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.utils;

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.splitters.*;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.GenericJdbcManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to
 * demarcate splits, it tries to generate WHERE clauses which separate the
 * data into roughly equivalent shards.
 */
public class DataDrivenDBInputFormat<T extends DBWritable>//, InputSplit
        extends DBInputFormat<T> {

    private static Logger LOG = LoggerFactory.getLogger(DataDrivenDBInputFormat.class);

    /**
     * If users are providing their own query, the following string is expected
     * to appear in the WHERE clause, which will be substituted with a pair of
     * conditions on the input to allow input splits to parallelise the import.
     */
    public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

    /**
     * @return the DBSplitter implementation to use to divide the table/query
     * into InputSplits.
     */
    public DBSplitter getSplitter(int sqlDataType) {
        return getSplitter(sqlDataType, 0, null);
    }

    /**
     * @return the DBSplitter implementation to use to divide the table/query
     * into InputSplits.
     */
    public DBSplitter getSplitter(int sqlDataType, long splitLimit, String rangeStyle) {
        switch (sqlDataType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return new BigDecimalSplitter();

            case Types.BIT:
            case Types.BOOLEAN:
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return new BooleanSplitter();

            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                return new IntegerSplitter();

            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return new FloatSplitter();

            case Types.NVARCHAR:
            case Types.NCHAR:
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return new NTextSplitter(rangeStyle);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return new TextSplitter(rangeStyle);

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return new DateSplitter();

            default:
                // TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB,
                // BLOB, ARRAY, STRUCT, REF, DATALINK, and JAVA_OBJECT.
                if (splitLimit >= 0) {
                    throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
                }
                return null;
        }
    }


    public Map<String, Object> getSplits(String splitCol, GenericJdbcManager dbManager, String dataSourceInfo, ZkService zkService) throws Exception {
        Map<String, Object> allInfoMap = new HashMap<>();
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        DBConfiguration dbConfiguration = getDBConf();

        //分片大小
        int splitShardSize = dbConfiguration.getSplitShardSize();
        //构建monitor节点路径
        String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
        //获取物理表
        String[] physicalTables = dbConfiguration.getString(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY)
                .split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER);
        //获取表分区信息
        Map tablePartitionsMap = (Map) dbConfiguration.getConfProperties()
                .get(DBConfiguration.TABEL_PARTITIONS);
        LOG.info("Physical Tables count:{}; ", tablePartitionsMap.size());

        boolean hasNotProperSplitCol = StringUtils.isBlank(splitCol);
        long totalRows = 0;
        int totalShardsCount = 0;
        if (hasNotProperSplitCol) {
            //如果不分片的情况
            for (String table : physicalTables) {
                List<String> tablePartitions = (List<String>) tablePartitionsMap.get(table);
                LOG.info("{} Partitions count: {}.", table, tablePartitions.size());
                for (String tablePartition : tablePartitions) {
                    long totalRowsOfCurShard = dbManager.queryTotalRows(table, splitCol, tablePartition);
                    totalRows = totalRows + totalRowsOfCurShard;
                    totalShardsCount++;
                    //每算出一个分表情况 就更新一次
                    FullPullHelper.updateMonitorSplitPartitionInfo(zkService, progressInfoNodePath, totalShardsCount, totalRows);
                    LOG.info("Physical Table:{}.{} - curRows: {}, totalRows: {}", table, tablePartition, totalRowsOfCurShard, totalRows);

                    InputSplit inputSplit = new DataDrivenDBInputFormat.DataDrivenDBInputSplit(-1, "1", " = ", "1",
                            " = ", "1");
                    inputSplit.setTargetTableName(table);
                    inputSplit.setTablePartitionInfo(tablePartition);
                    inputSplitList.add(inputSplit);
                }
            }
            LOG.info("Not found proper column for splitting. Will generate 1=1 split(s).");

            allInfoMap.put(Constants.TABLE_SPLITTED_TOTAL_ROWS_KEY, totalRows);
            allInfoMap.put(Constants.TABLE_SPLITTED_SHARD_SPLITS_KEY, inputSplitList);
            return allInfoMap;

        } else {
            String pullCollate = "";
            String dsType = dbConfiguration.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            // 仅mysql需要考虑设置collate
            if (dsType.toUpperCase().equals(DbusDatasourceType.MYSQL.name())) {
                pullCollate = FullPullHelper.getConfFromZk(Constants.ZkTopoConfForFullPull.COMMON_CONFIG,
                        DataPullConstants.PULL_COLLATE_KEY, false);
                pullCollate = (pullCollate != null) ? pullCollate : "";
            }

            String logicalTableName = dbConfiguration.getInputTableName();
            LOG.info("logicalTableName=" + logicalTableName);
            String splitterStyle = (String) dbConfiguration.get(DBConfiguration.SPLIT_STYLE);
            splitterStyle = StringUtils.isNotBlank(splitterStyle) ? splitterStyle : DataPullConstants.SPLITTER_STRING_STYLE_DEFAULT;

            LOG.info("splitterStyle=" + splitterStyle);
            if (splitterStyle.equals("md5") || splitterStyle.equals("number")) {
                pullCollate = "";
            }
            LOG.info("pullCollate=" + pullCollate);

            for (String table : physicalTables) {
                List<String> tablePartitions = (List<String>) tablePartitionsMap.get(table);
                LOG.info("{} Partitions count: {}.", table, tablePartitions.size());
                for (String tablePartition : tablePartitions) {
                    long totalRowsOfCurShard = dbManager.queryTotalRows(table, splitCol, tablePartition);
                    // 如果splitShardSize = -1，则指定分片数为1片
                    //int numSplitsOfCurShard = 1;
                    // 如果splitShardSize 不为 -1，计算分片数
                    //if (splitShardSize != -1) {
                    //    // 为减少和客户的约定，不要求客户提交分片数目。此处分片数目利用fetchsize计算得来
                    //    numSplitsOfCurShard = totalRowsOfCurShard % splitShardSize == 0
                    //            ? (int) (totalRowsOfCurShard / splitShardSize) : (int) (totalRowsOfCurShard / splitShardSize + 1);
                    //}

                    int numSplitsOfCurShard = totalRowsOfCurShard % splitShardSize == 0
                            ? (int) (totalRowsOfCurShard / splitShardSize) : (int) (totalRowsOfCurShard / splitShardSize + 1);
                    totalRows = totalRows + totalRowsOfCurShard;
                    totalShardsCount = totalShardsCount + numSplitsOfCurShard;

                    inputSplitList.addAll(dbManager.querySplits(table, splitCol, tablePartition, splitterStyle, pullCollate,
                            numSplitsOfCurShard, this));

                    FullPullHelper.updateMonitorSplitPartitionInfo(zkService, progressInfoNodePath, totalShardsCount, totalRows);
                    LOG.info("Physical Table:{}.{} - curRows: {}, totalRows: {}, Shards count: {}, Split Shard Size:{}.", table, tablePartition,
                            totalRowsOfCurShard, totalRows, numSplitsOfCurShard, splitShardSize);
                }
            }

            // allInfoMap.put(Constants.TABLE_SPLITTED_SHARDS_COUNT_KEY,
            // inputSplitList.size());
            LOG.info("All splits are generated. Ready for writing kafka now.");

            allInfoMap.put(Constants.TABLE_SPLITTED_TOTAL_ROWS_KEY, totalRows);
            allInfoMap.put(Constants.TABLE_SPLITTED_SHARD_SPLITS_KEY, inputSplitList);

            return allInfoMap;
        }
    }

    /*
     * Set the user-defined bounding query to use with a user-defined query.
     * This *must* include the substring "$CONDITIONS"
     * (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause,
     * so that DataDrivenDBInputFormat knows where to insert split clauses.
     * e.g., "SELECT foo FROM mytable WHERE $CONDITIONS"
     * This will be expanded to something like:
     * SELECT foo FROM mytable WHERE (id &gt; 100) AND (id &lt; 250)
     * inside each split.
     */

    /**
     * Fix bug by Dbus team 20161230
     * A InputSplit that spans a set of rows.
     */
    public static class DataDrivenDBInputSplit
            extends DBInputFormat.DBInputSplit {

        private int sqlType;
        private String splitCol;
        private String lowerOperator;
        private Object lowerValue;
        private String upperOperator;
        private Object upperValue;

        /**
         * Default Constructor.
         */
        public DataDrivenDBInputSplit() {
        }

        /**
         * Convenience Constructor.
         */
        public DataDrivenDBInputSplit(int sqlType, final String splitCol, final String lowerOperator,
                                      final Object lowerValue, final String upperOperator, final Object upperValue) {
            this.sqlType = sqlType;
            this.splitCol = splitCol;
            this.lowerOperator = lowerOperator;
            this.lowerValue = lowerValue;
            this.upperOperator = upperOperator;
            // this.upperOperator = " collate utf8_bin "+upperOperator;
            this.upperValue = upperValue;
        }

        public String getCondWithPlaceholder() {
            String collate = "";
            if (Types.VARCHAR == this.sqlType) {
                collate = super.getCollate();
            }
            StringBuffer sb = new StringBuffer();
            sb.append(this.splitCol).append(collate).append(lowerOperator);
            if (!DataPullConstants.QUERY_COND_IS_NULL.equals(lowerOperator)) {
                sb.append("?");
            }
            sb.append(" AND ").append(this.splitCol).append(collate).append(upperOperator);
            if (!DataPullConstants.QUERY_COND_IS_NULL.equals(upperOperator)) {
                sb.append("?");
            }
            return sb.toString();
            // return this.splitCol + collate + lowerOperator + "? AND " + this.splitCol + collate + upperOperator + "?";
        }

        public String getSplitCol() {
            return splitCol;
        }

        public String getLowerOperator() {
            return lowerOperator;
        }

        public Object getLowerValue() {
            return lowerValue;
        }

        public String getUpperOperator() {
            return upperOperator;
        }

        public Object getUpperValue() {
            return upperValue;
        }

        public void setSplitCol(String splitCol) {
            this.splitCol = splitCol;
        }

        public void setLowerOperator(String lowerOperator) {
            this.lowerOperator = lowerOperator;
        }

        public void setLowerValue(Object lowerValue) {
            this.lowerValue = lowerValue;
        }

        public void setUpperOperator(String upperOperator) {
            this.upperOperator = upperOperator;
        }

        public void setUpperValue(Object upperValue) {
            this.upperValue = upperValue;
        }

        public int getSqlType() {
            return sqlType;
        }

        public void setSqlType(int sqlType) {
            this.sqlType = sqlType;
        }
    }
}
