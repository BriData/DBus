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


package com.creditease.dbus.manager;


import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputFormat;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.common.splitters.DBSplitter;
import com.creditease.dbus.enums.DbusDatasourceType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class GenericSqlManager extends GenericConnManager implements SqlManager {
    private static Logger logger = LoggerFactory.getLogger(GenericSqlManager.class);

    /**
     * Constructs the GenericSqlManager.
     * * @param driverClass
     *
     * @param dbConfig
     * @param connectString
     */
    public GenericSqlManager(final String driverClass, final DBConfiguration dbConfig, String connectString) {
        super(driverClass, dbConfig, connectString);
    }

    /**
     * Return a list of column names in a table in the order returned by the db.
     */
    @Override
    public String[] getColumnNames(String tableName) {
        return null;
    }

    /**
     * When using a column name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a column named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param colName the column name as provided by the user, etc.
     * @return how the column name should be rendered in the sql text.
     */
    @Override
    public String escapeColName(String colName) {
        return colName;
    }

    /**
     * When using a table name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a table named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param tableName the table name as provided by the user, etc.
     * @return how the table name should be rendered in the sql text.
     */
    @Override
    public String escapeTableName(String tableName) {
        return null;
    }

    /**
     * Determine what column to use to split the table.
     *
     * @return the splitting column, if one is set or inferrable, or null
     * otherwise.
     */
    @Override
    public String getSplitColumn() {
        String splitCol = dbConfig.getString(DBConfiguration.INPUT_SPLIT_COL);
        if (StringUtils.isNotBlank(splitCol)) {
            return splitCol;
        }
        String tableName = dbConfig.getString(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_KEY);
        // 对于系列表，任取其中一个表来获取Meta信息。此处取第一个表。
        if (tableName.indexOf(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            tableName = tableName.split(FullPullConstants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }
        if (tableName != null) {
            splitCol = queryIndexedColumn(tableName, FullPullConstants.SPLIT_COL_TYPE_PK);
            if (null == splitCol) {
                splitCol = queryIndexedColumn(tableName.toUpperCase(), FullPullConstants.SPLIT_COL_TYPE_PK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColumn(tableName, FullPullConstants.SPLIT_COL_TYPE_UK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColumn(tableName.toUpperCase(), FullPullConstants.SPLIT_COL_TYPE_UK);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColumn(tableName, FullPullConstants.SPLIT_COL_TYPE_COMMON_INDEX);
            }
            if (null == splitCol) {
                splitCol = queryIndexedColumn(tableName.toUpperCase(), FullPullConstants.SPLIT_COL_TYPE_COMMON_INDEX);
            }
        }

        if (StringUtils.isBlank(splitCol)) {
            splitCol = "";
        }
        dbConfig.set(DBConfiguration.INPUT_SPLIT_COL, splitCol);
        logger.info("[split bolt] getSplitColumn() set split col is : {}", splitCol);
        return splitCol;
    }

    @Override
    public long queryTotalRows(String table, String splitCol, String tablePartition) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        long totalCountOfCurShard = 0;
        try {
            String query = getTotalRowsCountQuery(table, splitCol, tablePartition);
            logger.info("[split bolt] queryTotalRows(), query: {}", query);
            conn = getConnection();
            ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            if (rs.next()) {
                totalCountOfCurShard = rs.getLong("TOTALCOUNT");
            }
            logger.info("[split bolt] queryTotalRows(), query: {}, totalRows : {}", query, totalCountOfCurShard);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            close(ps, rs);
        }
        return totalCountOfCurShard;
    }

    @Override
    public List<InputSplit> querySplits(String table, String splitCol, String tablePartition, String splitterStyle, String pullCollate,
                                        long numSplitsOfCurShard) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<InputSplit> inputSplitListOfCurShard = null;
        try {
            String query = getBoundingValsQuery(table, splitCol, tablePartition);
            logger.info("[split bolt] 分片列边界值查询: " + query);

            conn = getConnection();
            ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            int sqlDataType = rs.getMetaData().getColumnType(1);
            if (rs.next()) {
                boolean isSigned = rs.getMetaData().isSigned(1);
                if (sqlDataType == Types.INTEGER && !isSigned) {
                    sqlDataType = Types.BIGINT;
                }
            }
            DBSplitter splitter = DataDBInputFormat.getSplitter(sqlDataType, dbConfig.getInputSplitLimit(), splitterStyle);
            if (null == splitter) {
                throw new IOException("Does not have the splitter for the given"
                        + " SQL data type. Please use either different split column (argument"
                        + " --split-by) or lower the number of mappers to 1. Unknown SQL data"
                        + " type: " + sqlDataType);
            }
            try {
                inputSplitListOfCurShard = splitter.split(numSplitsOfCurShard, rs, splitCol, dbConfig);
                for (InputSplit inputSplit : inputSplitListOfCurShard) {
                    inputSplit.setTargetTableName(table);
                    inputSplit.setCollate(pullCollate);
                    inputSplit.setTablePartitionInfo(tablePartition);
                }
                logger.info("[split bolt] table:{}.{} ,分片数量{}片, min({}):{} ,max({}):{}", table, tablePartition,
                        numSplitsOfCurShard, splitCol, rs.getString(1), splitCol, rs.getString(2));
            } catch (Exception e) {
                throw new IOException(e);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            close(ps, rs);
        }
        return inputSplitListOfCurShard;
    }

    @Override
    public List<String> queryTablePartitions(String sql) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> partitionsList = new ArrayList<>();
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            while (rs.next()) {
                partitionsList.add((String) rs.getObject(1));
            }
        } catch (SQLException e) {
            logger.warn("Encountered exception when processing partions of table. Just ignore partition or confirm if you are authorized to access table DBA_TAB_PARTITIONS.");
        } catch (Exception e) {
            logger.error("Encountered exception when processing partions of table.");
        } finally {
            close(ps, rs);
        }
        return partitionsList;
    }

    public String getIndexedColQuery(String indexType) {
        return null;
    }

    public String queryIndexedColumn(String tableName, String indexType) {
        String splitCol = null;
        Connection conn = null;
        PreparedStatement ps = null;
        PreparedStatement psOracle = null;
        ResultSet rs = null;
        ResultSet rsOracle = null;
        List<String> columns = new ArrayList<>();

        String schema = null;
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            schema = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }
        try {
            conn = getConnection();
            String indexedColQuery = getIndexedColQuery(indexType);
            logger.info("[split bolt] table {}.{}, sql for queryIndexedColumn : {}.", schema, shortTableName, indexedColQuery);

            ps = conn.prepareStatement(indexedColQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setString(1, shortTableName);
            ps.setString(2, schema);
            rs = ps.executeQuery();

            while (rs.next()) {
                columns.add(rs.getString(1));
            }

            if (columns.size() == 0) {
                logger.warn("[split bolt] Table has no key, type:" + indexType);
                return null;
            } else if (columns.size() == 1) {
                logger.info("[split bolt] find split column :" + columns.get(0) + ", Index type:" + indexType);
            } else {
                logger.warn("[split bolt] The table " + tableName + " " + "contains a multi-column key. Will default to "
                        + "the column " + columns.get(0) + " only for this job." + ", type:" + indexType);
            }

            splitCol = columns.get(0);

            String datasourceType = dbConfig.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
            if (dataBaseType == DbusDatasourceType.ORACLE) {
                // 对于ORACLE数据库，目前只有整数类型比较友好。区别对待下整数类型分片列和其他类型分片列
                String splitColTypeDetectQuery = "select " + splitCol + " from " + tableName + " where rownum <= 1";
                psOracle = conn.prepareStatement(splitColTypeDetectQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                rsOracle = psOracle.executeQuery();

                while (rsOracle.next()) {
                    int splitColSqlDataType = rsOracle.getMetaData().getColumnType(1);
                    if (splitColSqlDataType == Types.INTEGER
                            || splitColSqlDataType == Types.TINYINT
                            || splitColSqlDataType == Types.SMALLINT
                            || splitColSqlDataType == Types.BIGINT
                            || splitColSqlDataType == Types.NUMERIC
                            || splitColSqlDataType == Types.DECIMAL
                            || splitColSqlDataType == Types.REAL
                            || splitColSqlDataType == Types.FLOAT
                            || splitColSqlDataType == Types.DOUBLE) {
                        // 对于上述数字类型，DBUS根据 splitCol 按分片策略分片并发拉取。
                        // 此处故意留白
                        logger.info("Found split column data type is {}(Numeric):", splitColSqlDataType);
                    } else {
                        // 对于整数以外的其它类型，将splitCol设为null。后续逻辑认为没有合适的分片列。将不对数据进行分片，所有数据作一片拉取。
                        splitCol = null;
                        logger.info("Found split column data type is {}(None Numeric):", splitColSqlDataType);
                    }
                }
            } else if (dataBaseType == DbusDatasourceType.DB2) {
                String[] split = splitCol.split("\\+");
                splitCol = split[1];
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            close(ps, rs);
            close(psOracle, rsOracle);
        }
        return splitCol;
    }


    /**
     * 使用connection 返回一个 statment用于后续使用，改statement 由lastStatement 管理
     *
     * @param stmt
     * @return
     * @throws Exception
     */
    public PreparedStatement prepareStatement(String stmt) throws Exception {
        release();

        PreparedStatement statement = this.getConnection().prepareStatement(stmt,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.lastStatement = statement;

        return statement;
    }

    private String getTotalRowsCountQuery(String table, String splitCol, String tablePartition) {
        StringBuilder query = new StringBuilder();

        if (StringUtils.isNotBlank(splitCol)) {
            query.append("SELECT COUNT(").append(splitCol).append(") TOTALCOUNT FROM ");
        } else {
            query.append("SELECT COUNT(*) TOTALCOUNT FROM ");
        }
        query.append(table);
        if (StringUtils.isNotBlank(tablePartition)) {
            query.append(" PARTITION (").append(tablePartition).append(") ");
        }
        // 现在不再处理SCN
        //Object consistentReadScn = dbConfig.get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
        //if (consistentReadScn != null) {
        //    query.append(" AS OF SCN ").append((Long) consistentReadScn).append(" ");
        //}
        String conditions = dbConfig.getInputConditions();
        if (null != conditions) {
            query.append(" WHERE ( " + conditions + " )");
        }
        return query.toString();
    }

    /**
     * @return a query which returns the minimum and maximum values for
     * the order-by column.
     * <p>
     * The min value should be in the first column, and the
     * max value should be in the second column of the rs.
     */
    private String getBoundingValsQuery(String table, String splitCol, String tablePartition) {
        // Auto-generate one based on the table name we've been provided with.
        StringBuilder query = new StringBuilder();

        query.append("SELECT MIN(").append(splitCol).append("), ");
        query.append("MAX(").append(splitCol).append(") FROM ");
        query.append(table);
        if (StringUtils.isNotBlank(tablePartition)) {
            query.append(" PARTITION (").append(tablePartition).append(") ");
        }

        String conditions = dbConfig.getInputConditions();
        if (StringUtils.isNotBlank(conditions)) {
            query.append(" WHERE ( " + conditions + " )");
        }

        return query.toString();
    }


    /**
     * 统一资源关闭处理
     *
     * @param ps
     * @param rs
     * @return
     * @throws SQLException
     */
    public static void close(PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            logger.error("GenericSqlManager close resource exception", e);
        }
    }
}
