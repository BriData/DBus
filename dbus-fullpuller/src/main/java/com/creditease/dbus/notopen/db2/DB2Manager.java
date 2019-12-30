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
package com.creditease.dbus.notopen.db2;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.commons.SupportedDb2DataType;
import com.creditease.dbus.manager.GenericSqlManager;
import com.creditease.dbus.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DB2Manager extends GenericSqlManager {

    private static Logger logger = LoggerFactory.getLogger(DB2Manager.class);

    public static final String DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver";
    /**
     * Query to find the primary key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
            "SELECT COLNAMES FROM SYSCAT.INDEXES WHERE TABNAME=? AND TABSCHEMA=? AND UNIQUERULE ='P'";

    /**
     * Query to find the UNIQUE key column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
            "SELECT COLNAMES FROM SYSCAT.INDEXES WHERE TABNAME=? AND TABSCHEMA=? AND UNIQUERULE ='U'";

    /**
     * Query to find the INDEXED column name for a given table. This query
     * is restricted to the current schema.
     */
    public static final String QUERY_INDEXED_COL_FOR_TABLE =
            "SELECT COLNAMES FROM SYSCAT.INDEXES WHERE TABNAME=? AND TABSCHEMA=? AND UNIQUERULE ='D'";

    public DB2Manager(final DBConfiguration opts, String conString) {
        super(DRIVER_CLASS, opts, conString);
    }

    public String getIndexedColQuery(String indexType) {
        if (FullPullConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (FullPullConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (FullPullConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    @Override
    public String[] getColumnNames(String tableName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<String>();
        String schema = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
        String sql = "SELECT COLNAME,TYPENAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA='" + schema + "' AND TABNAME='" + tableName + "'";

        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString(1);
                String columnDataType = rs.getString(2);
                if (SupportedDb2DataType.isSupported(columnDataType)) {
                    columns.add(columnName.toUpperCase());
                }
            }
        } catch (SQLException sqle) {
            LoggingUtils.logAll(logger, "Failed to query meta from original DB.", sqle);
            throw new RuntimeException(sqle);
        } catch (Exception e) {
            logger.error("Failed to list columns from query:{}", sql);
        } finally {
            close(ps, rs);
        }
        return columns.toArray(new String[columns.size()]);
    }

}
