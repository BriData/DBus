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


import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * A RecordReader that reads records from an Oracle SQL table.
 */
public class OracleDBRecordReader extends DBRecordReader {

    /**
     * Configuration key to set to a timezone string.
     */
    public static final String SESSION_TIMEZONE_KEY = "oracle.sessionTimeZone";

    private Logger logger = LoggerFactory.getLogger(getClass());

    public OracleDBRecordReader(GenericConnManager manager, DBConfiguration dbConfig, DataDBInputSplit inputSplit,
                                String[] fields, String table) throws SQLException {
        super(manager, dbConfig, inputSplit, fields, table);
    }

    /**
     * Returns the query for selecting the records from an Oracle DB.
     */
    @Override
    public String getSelectQuery() throws Exception {
        String commonSelectQuery = getCommonSelectQuery();
        StringBuilder query = new StringBuilder(commonSelectQuery);
        return query.toString();
    }
}
