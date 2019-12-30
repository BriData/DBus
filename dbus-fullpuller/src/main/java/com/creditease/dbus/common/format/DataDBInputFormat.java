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
package com.creditease.dbus.common.format;

import com.creditease.dbus.common.splitters.*;

import java.sql.Types;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to
 * demarcate splits, it tries to generate WHERE clauses which separate the
 * data into roughly equivalent shards.
 */
public class DataDBInputFormat {

    /**
     * @return the DBSplitter implementation to use to divide the table/query
     * into InputSplits.
     */
    public static DBSplitter getSplitter(int sqlDataType, long splitLimit, String rangeStyle) {
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

}
