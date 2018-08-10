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
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.utils;

import java.sql.Types;

import com.creditease.dbus.common.splitters.DBSplitter;
import com.creditease.dbus.common.splitters.OracleDateSplitter;

/**
 * A InputFormat that reads input data from an SQL table in an Oracle db.
 */
public class OracleDataDrivenDBInputFormat<T extends DBWritable>
    extends DataDrivenDBInputFormat<T>{
  /**
   * @return the DBSplitter implementation to use to divide the table/query
   * into InputSplits.
   */
  @Override
  public DBSplitter getSplitter(int sqlDataType, long splitLimit, String rangeStyle) {
    switch (sqlDataType) {
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new OracleDateSplitter();

    default:
      return super.getSplitter(sqlDataType, splitLimit, rangeStyle);
    }
  }
}
