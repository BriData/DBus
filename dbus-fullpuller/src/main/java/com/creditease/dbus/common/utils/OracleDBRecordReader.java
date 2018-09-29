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

import com.creditease.dbus.common.utils.DataDrivenDBInputFormat.DataDrivenDBInputSplit;
import com.creditease.dbus.manager.GenericJdbcManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 * A RecordReader that reads records from an Oracle SQL table.
 */
public class OracleDBRecordReader<T extends DBWritable>
        extends DBRecordReader<T> {

    /**
     * Configuration key to set to a timezone string.
     */
    public static final String SESSION_TIMEZONE_KEY = "oracle.sessionTimeZone";

    private Logger LOG = LoggerFactory.getLogger(getClass());

    // CHECKSTYLE:OFF
    public OracleDBRecordReader(GenericJdbcManager manager,
                                DBConfiguration dbConfig, DataDrivenDBInputSplit inputSplit, String[] fields, String table) throws SQLException {
        super(manager, dbConfig, inputSplit, fields, table);
//    setSessionTimeZone(conf, conn);
    }
    // CHECKSTYLE:ON

    public String getMetaSql() {
        String tableName = getSplit().getTargetTableName();
        String tableOwner = null;
        String shortTableName = null;
        int qualifierIndex = tableName.indexOf('.');
        tableOwner = tableName.substring(0, qualifierIndex);
        shortTableName = tableName.substring(qualifierIndex + 1);

        return "SELECT COLUMN_NAME columnName, DATA_TYPE columnTypeName, DATA_PRECISION PRECISION, DATA_SCALE SCALE, " +
                "NULLABLE isNullable FROM ALL_TAB_COLUMNS WHERE  OWNER='" + tableOwner + "' AND TABLE_NAME = '" + shortTableName + "'";
    }

    /**
     * Returns the query for selecting the records from an Oracle DB.
     */
    @Override
    public String getSelectQuery() throws Exception {
        StringBuilder query = new StringBuilder();
        DBConfiguration dbConf = getDBConf();
        String tableName = getSplit().getTargetTableName();
        String[] fieldNames = getFieldNames();

        query.append("SELECT ");
        if (null != fieldNames && fieldNames.length != 0) {
            for (int i = 0; i < fieldNames.length; i++) {
                // query.append(OracleUtils.escapeIdentifier(fieldNames[i]));
                query.append(fieldNames[i]);
                if (i != fieldNames.length - 1) {
                    query.append(", ");
                }
            }
        } else {
            // query.append(" * ");
            throw new Exception("None supported columns found on current pulling target table");
        }

        query.append(" FROM ").append(tableName);
        if (StringUtils.isNotBlank(getSplit().getTablePartitionInfo())) {
            query.append(" PARTITION (").append(getSplit().getTablePartitionInfo()).append(") ");
        }
        Object consistentReadScn = getDBConf().get(DBConfiguration.DATA_IMPORT_CONSISTENT_READ_SCN);
        if (consistentReadScn != null) {
            query.append(" AS OF SCN ").append((Long) consistentReadScn).append(" ");
        }

        String conditions = dbConf.getInputConditions();
        boolean hasWhereKeyAlready = false;
        if (null != conditions) {
            query.append(" WHERE ( " + conditions + " )");
            hasWhereKeyAlready = true;
        }

        if (this.getSplit() != null) {
            if (hasWhereKeyAlready) {
                query.append(" AND ");
            } else {
                query.append(" WHERE ");
            }
            query.append(this.getSplit().getCondWithPlaceholder());
        }

        String orderBy = dbConf.getInputOrderBy();
        if (orderBy != null && orderBy.length() > 0) {
            query.append(" ORDER BY ").append(orderBy);
        }

        try {
            DBInputFormat.DBInputSplit split = getSplit();
            if (split.getLength() > 0 && split.getStart() > 0) {
                String querystring = query.toString();

                query = new StringBuilder();
                query.append("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( ");
                query.append(querystring);
                query.append(" ) a WHERE rownum <= ").append(split.getStart());
                query.append(" + ").append(split.getLength());
                query.append(" ) WHERE dbif_rno >= ").append(split.getStart());
            }
        } catch (IOException ex) {
            // ignore, will not throw.
        }
        return query.toString();
    }
}
