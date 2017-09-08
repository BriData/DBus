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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as
 * key and DBWritables as value.
 *
 * The SQL query, and input class can be using one of the two
 * setInput methods.
 */
public class DBInputFormat<T extends DBWritable> {

    private Logger LOG = LoggerFactory.getLogger(getClass());
  private String dbProductName = "DEFAULT";


  /**
   * A InputSplit that spans a set of rows.
   */
  public static class DBInputSplit extends InputSplit {

    private long end = 0;
    private long start = 0;
    
    /**
     * Default Constructor.
     */
    public DBInputSplit() {
    }

    /**
     * Convenience Constructor.
     * @param start the index of the first row to select
     * @param end the index of the last row to select
     */
    public DBInputSplit(long start, long end) {
      this.start = start;
      this.end = end;
    }

//    @Override
//    /** {@inheritDoc} */
//    public String[] getLocations() throws IOException {
//      // TODO Add a layer to enable SQL "sharding" and support locality
//      return new String[] {};
//    }

    /**
     * @return The index of the first row to select
     */
    public long getStart() {
      return start;
    }

    /**
     * @return The index of the last row to select
     */
    public long getEnd() {
      return end;
    }

    /**
     * @return The total row count in this split
     */
    public long getLength() throws IOException {
      return end - start;
    }
  }

  private String conditions;

  private Connection connection;

  private String tableName;

//  private String[] fieldNames;

  private DBConfiguration dbConf;

//  @Override
  /** {@inheritDoc} */
  public void setConf(DBConfiguration dbConf) {

    this.dbConf = dbConf;
//    try {
//      getConnection();
//    } catch (Exception ex) {
//      throw new RuntimeException(ex);
//    }

    this.tableName = dbConf.getInputTableName();
//    this.fieldNames = dbConf.getInputFieldNames();
    this.conditions = dbConf.getInputConditions();
  }

  private void setTxIsolation(Connection conn) {
    try {

      if (this.dbConf.getBoolean(DBConfiguration.PROP_RELAXED_ISOLATION, false)) {
     
        if (dbProductName.startsWith("ORACLE")) {
          LOG.info("Using read committed transaction isolation for Oracle"
            + " as read uncommitted is not supported");
          this.connection.setTransactionIsolation(
            Connection.TRANSACTION_READ_COMMITTED);
        } else {
          LOG.info("Using read uncommited transaction isolation");
          this.connection.setTransactionIsolation(
            Connection.TRANSACTION_READ_UNCOMMITTED);
        }
      }
      else {
        LOG.info("Using read commited transaction isolation");
        this.connection.setTransactionIsolation(
          Connection.TRANSACTION_READ_COMMITTED);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public DBConfiguration getDBConf() {
    return dbConf;
  }

  public String getDBProductName() {
    return dbProductName;
  }
  
  /** Returns the query for getting the total number of rows,
   * subclasses can override this for custom behaviour.*/
  protected String getCountQuery() {

    if (dbConf.getInputCountQuery() != null) {
      return dbConf.getInputCountQuery();
    }

    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM " + tableName);

    if (conditions != null && conditions.length() > 0) {
      query.append(" WHERE " + conditions);
    }
    return query.toString();
  }

  protected void closeConnection() {
    try {
      if (null != this.connection) {
        this.connection.close();
        this.connection = null;
      }
    } catch (SQLException sqlE) { /* ignore exception on close. */ }
  }

}
