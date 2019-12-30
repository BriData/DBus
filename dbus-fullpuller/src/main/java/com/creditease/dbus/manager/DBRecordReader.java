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
package com.creditease.dbus.manager;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.splitters.DateSplitter;
import com.creditease.dbus.utils.LoggingUtils;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

/**
 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class DBRecordReader {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private DataDBInputSplit split;

    private GenericConnManager manager;

    private DBConfiguration dbConf;

    private String[] fieldNames;

    private String tableName;

    public DBRecordReader(GenericConnManager manager, DBConfiguration dbConfig, DataDBInputSplit split,
                          String[] fields, String table) {
        this.manager = manager;
        this.dbConf = dbConfig;
        this.split = split;
        if (fields != null) {
            this.fieldNames = Arrays.copyOf(fields, fields.length);
        }
        this.tableName = table;
    }

    public ResultSet queryData(String splitIndex) throws Exception {
        ResultSet rs = null;
        try {
            Object lowBound = this.split.getLowerValue();
            Object upperBound = this.split.getUpperValue();

            if (lowBound instanceof String && upperBound instanceof String) {
                String lower = (String) lowBound;
                String upper = (String) upperBound;
                for (int i = 0; i < lower.length(); i++) {
                    if (lower.charAt(i) > (int) 0xffff) {
                        throw new Exception("Exception:lower char is wrong" + lower.charAt(i));
                    }
                }
                for (int i = 0; i < upper.length(); i++) {
                    if (upper.charAt(i) > (int) 0xffff) {
                        throw new Exception("Exception:upper char is wrong" + upper.charAt(i));
                    }
                }
            }

            String query = getSelectQuery();

            PreparedStatement statement = manager.prepareStatement(query);
            // cond 不为 is null的时候，才用set 条件值。另：lower is null, upper一定也is null.所以不用两个都判断。
            if (!FullPullConstants.QUERY_COND_IS_NULL.equals(this.split.getLowerOperator())) {
                if (this.split.getSqlType() == Types.DATE || this.split.getSqlType() == Types.TIME || this.split.getSqlType() == Types.TIMESTAMP) {
                    // Mysql：Time类型，TIMESTAMP类型，Date类型：都可以用这种方式处理.
                    // Oracle:沒有Time类型。TIMESTAMP类型可用这种方式处理，Date类型，不能用这种方式处理（会丢失其所含的时分秒毫秒信息），下面会单独处理。
                    lowBound = DateSplitter.longToDate((long) this.split.getLowerValue(), this.split.getSqlType());
                    upperBound = DateSplitter.longToDate((long) this.split.getUpperValue(), this.split.getSqlType());
                }
                /*这段代码已经不会走到了。对于Oracle Types.DATE，DateSplitter类中已经将其强制转换成Types.TIMESTAMP。暂时保留便于了解相关细节。
                 Date类型，oracle需特殊处理下。Oracle的Date类型不仅保存年月日，还能保存时分秒甚至毫秒信息。
                 但Oracle通过resultSet.getObject获取时间时，可能遭到截断，丢失时分秒（http://www.myexception.cn/database/1044846.html）
                 例：对于 2008-06-13 13:48:21.0， 9i/11g返回2008-06-13 13:48:21.0；10g返回2008-06-13
                 这里提到的版本指数据库服务器版本。同样版本的jdbc，连不同环境的Oracle服务器，同样的类型和数据，返回值不一样。
                 提示说可用prop.setProperty("oracle.jdbc.V8Compatible" ,"true");解决。在有问题的环境验证，没解决问题。所以采取了强制转换为Types.TIMESTAMP的方式处理。
                if (datasourceType.toUpperCase().equals(DbusDatasourceType.ORACLE.name()) && this.split.getSqlType() == Types.DATE) {
                    logger.info("DbusDatasourceType.ORACLE------------ SqlType{}  ",this.split.getSqlType());
                    SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    lowBound = dfs.format(new java.util.Date((long)this.split.getLowerValue()));
                    upperBound = dfs.format(new java.util.Date((long)this.split.getUpperValue()));
                    logger.info("lower: {} and upper: {}.",lowBound,upperBound);
                }*/
                statement.setObject(1, lowBound, split.getSqlType());
                statement.setObject(2, upperBound, split.getSqlType());
            }

            logger.info("[pull bolt] index{}: Query Begin: {}, with cond lower: {} and upper: {}.", splitIndex, query, lowBound, upperBound);

            int fetchSize = dbConf.getPrepareStatementFetchSize();
            statement.setFetchSize(fetchSize);
            statement.setQueryTimeout(3600);
            logger.info("[pull bolt] index{}: Using fetchSize for next query: {},Using queryTimeout 3600 seconds.", splitIndex, fetchSize);

            Long startTime = System.currentTimeMillis();
            logger.info("[pull bolt] index{}: executeQuery start.", splitIndex);
            rs = statement.executeQuery();
            logger.info("[pull bolt] index{}: executeQuery end ,cost time {}", splitIndex, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
            return rs;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            LoggingUtils.logAll(logger, "[pull bolt] index" + splitIndex + " Failed to list columns", e);
            throw e;
        }
    }

    /**
     * Returns the query for selecting the records,
     */
    protected String getCommonSelectQuery() throws Exception {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        if (null != fieldNames && fieldNames.length != 0) {
            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);
                if (i != fieldNames.length - 1) {
                    query.append(", ");
                }
            }
        } else {
            throw new Exception("None supported columns found on current pulling target table");
        }
        query.append(" FROM ").append(split.getTargetTableName());
        if (StringUtils.isNotBlank(getSplit().getTablePartitionInfo())) {
            query.append(" PARTITION (").append(getSplit().getTablePartitionInfo()).append(") ");
        }
        query.append(" WHERE 1=1 ");

        String condWithPlaceholder = split.getCondWithPlaceholder();
        if (StringUtils.isNotBlank(condWithPlaceholder)) {
            query.append(" AND (").append(condWithPlaceholder).append(") ");
        }

        String inputConditions = dbConf.getInputConditions();
        if (StringUtils.isNotBlank(inputConditions)) {
            query.append(" AND (").append(inputConditions).append(") ");
        }
        String orderBy = dbConf.getInputOrderBy();
        if (orderBy != null && orderBy.length() > 0) {
            query.append(" ORDER BY ").append(orderBy);
        }
        return query.toString();
    }

    /**
     * Returns the query for selecting the records,
     * subclasses can override this for custom behaviour.
     *
     * @throws Exception
     */
    protected String getSelectQuery() throws Exception {
        String commonSelectQuery = getCommonSelectQuery();
        StringBuilder query = new StringBuilder(commonSelectQuery);
        return query.toString();
    }

    protected DataDBInputSplit getSplit() {
        return split;
    }

    protected String[] getFieldNames() {
        return fieldNames;
    }

    protected String getTableName() {
        return tableName;
    }

    protected DBConfiguration getDBConf() {
        return dbConf;
    }

    public void setSplit(DataDBInputSplit split) {
        this.split = split;
    }

}
