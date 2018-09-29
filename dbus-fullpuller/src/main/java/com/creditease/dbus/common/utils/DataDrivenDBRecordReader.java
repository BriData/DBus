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


import java.io.IOException;
import java.sql.SQLException;

import com.creditease.dbus.manager.GenericJdbcManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RecordReader that reads records from a SQL table,
 * using data-driven WHERE clause splits.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
@Deprecated
public class DataDrivenDBRecordReader<T extends DBWritable>
    extends DBRecordReader<T> {

    private Logger LOG = LoggerFactory.getLogger(getClass());

  private String dbProductName; // database manufacturer string.

  // CHECKSTYLE:OFF
  // TODO(aaron): Refactor constructor to use fewer arguments.
  /**
   * @param split The InputSplit to read data for
   * @throws SQLException
   */
  public DataDrivenDBRecordReader(DataDrivenDBInputFormat.DataDrivenDBInputSplit split, GenericJdbcManager manager,
      DBConfiguration dbConfig, String [] fields, String table,
      String dbProduct) throws SQLException {
    super(manager, dbConfig, split, fields, table);
    this.dbProductName = dbProduct;
  }
  
  // CHECKSTYLE:ON

//  @Override
//  /** {@inheritDoc} */
//  public float getProgress() throws IOException {
//    return isDone() ? 1.0f : 0.0f;
//  }

  /** Returns the query for selecting the records, with lower and upper
   * clause consitions provided as parameters
   * This is needed for recovering from connection failures after some data
   * in the split have been already processed */
  protected String getSelectQuery() {
      StringBuilder query = new StringBuilder();
      String [] fieldNames = getFieldNames();
      query.append("SELECT ");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      query.append(" FROM ").append(this.getSplit().getTargetTableName());      
      if(StringUtils.isNotBlank(getSplit().getTablePartitionInfo())){
          query.append(" PARTITION (").append(getSplit().getTablePartitionInfo()).append(") ");
      }
      if (!dbProductName.startsWith("ORACLE")
          && !dbProductName.startsWith("MICROSOFT SQL SERVER")
          && !dbProductName.startsWith("POSTGRESQL")) {
        // The AS clause is required for hsqldb. Some other databases might have
        // issues with it, so we're skipping some of them.
        query.append(" AS ").append(this.getSplit().getTargetTableName());
      }
      query.append(" WHERE ").append(this.getSplit().getCondWithPlaceholder());
//      query.append(" LIMIT 50");
      LOG.debug("Using query: " + query.toString());

      return query.toString();
  }
}
