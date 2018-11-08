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
package com.creditease.dbus.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.creditease.dbus.common.FullPullHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.LoggingUtils;

/**
 * Database manager that is connects to a generic JDBC-compliant
 * database; its constructor is parameterized on the JDBC Driver
 * class to load.
 */
public class GenericJdbcManager extends SqlManager {

    private Logger LOG = LoggerFactory.getLogger(getClass());

    private String jdbcDriverClass;
    private Connection connection;

    //统计 打开的连接数
    private static AtomicInteger totalRefCount = new AtomicInteger(0);
    private ThreadLocal<Integer> threadRefCountHolder = new ThreadLocal<>();


    public GenericJdbcManager(final String driverClass, final DBConfiguration opts, String conString) {
        super(opts, conString);

        this.jdbcDriverClass = driverClass;

        threadRefCountHolder.set(0);
  }

  @Override
  public Connection getConnection() throws SQLException, Exception {
      String whoami = getWhoamI();

      if (null == this.connection) {
          this.connection = makeConnection();

          int totalCount = totalRefCount.incrementAndGet();
          threadRefCountHolder.set(threadRefCountHolder.get() + 1);
          int threadCount = threadRefCountHolder.get();
          LOG.info("new JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);

          return this.connection;
      } else {
          int totalCount = totalRefCount.get();
          int threadCount = threadRefCountHolder.get();
          LOG.info("get JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);

          return this.connection;
      }
  }

  protected boolean hasOpenConnection() {
    return this.connection != null;
  }

  /**
   * Any reference to the connection managed by this manager is nulled.
   * If doClose is true, then this method will attempt to close the
   * connection first.
   * @param doClose if true, try to close the connection before forgetting it.
   */
//  public void discardConnection(boolean doClose) {
//    if (doClose && hasOpenConnection()) {
//      try {
//              this.connection.close();
//      } catch(SQLException sqe) {
//
//      }
//    }
//    this.connection = null;
//  }

  public String getWhoamI() {
      return Thread.currentThread().getName() + "_" + Thread.currentThread().getId();
  }

  public void close() throws SQLException {
      String whoami = getWhoamI();

      super.close();
      if (this.connection != null) {
          this.connection.close();
          this.connection = null;

          int totalCount = totalRefCount.decrementAndGet();
          threadRefCountHolder.set(threadRefCountHolder.get() - 1);
          int threadCount = threadRefCountHolder.get();
          LOG.info("delete JDBCConnection [{}]  threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);
      } else {
          this.connection = null;

          int totalCount = totalRefCount.get();
          int threadCount = threadRefCountHolder.get();
          LOG.info("set null JDBCConnection [{}] threadCount: {}, totalCount: {}", whoami, threadCount, totalCount);
      }
  }

  public String getDriverClass() {
    return jdbcDriverClass;
  }

  @Override
  public ResultSet executeSql(String sql) {
      
      Connection conn = null;
      PreparedStatement pStmt = null;
      ResultSet rset = null; 

      try {
        conn = getConnection();
        pStmt = conn.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rset = pStmt.executeQuery();
        conn.commit();
      } catch (SQLException e) {
        try {
          if (conn != null) {
            conn.rollback();
          }
        } catch (SQLException ex) {
          LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
        }
        LoggingUtils.logAll(LOG, "Failed to list columns", e);
      }
    catch (Exception e) {
        LOG.error(e.getMessage(), e);
    }
//      finally {
//        // rset业务调用方要用。用完由业务方关闭
//        if (rset != null) {
//          try {
//            rset.close();
//          } catch (SQLException ex) {
//            LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
//          }
//        }
//        if (pStmt != null) {
//          try {
//            pStmt.close();
//          } catch (SQLException ex) {
//            LoggingUtils.logAll(LOG, "Failed to close statement", ex);
//          }
//        }
// 关闭connecttion的事由druid去做
//        try {
//          close();
//        } catch (SQLException ex) {
//          LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//        }
//        catch (Exception e) {
//            // TODO Auto-generated catch block
//            LOG.error(e.getMessage(),e);
//        }
//      }

      return rset;
    }  
 
  @Override
  public String getIndexedColQuery(String indexType){
      // TODO
      return null;
  }
  
  @Override
  public List<String> queryTablePartitions(String sql) {
      Connection conn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      List<String> partitionsList = new ArrayList<>();
      try {
          conn = getConnection();
          pStmt = conn.prepareStatement(sql,
                  ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          rs = pStmt.executeQuery();
          while (rs.next()) {
              partitionsList.add((String) rs.getObject(1));
          }
          conn.commit();
      } catch (SQLException e) {
          try {
              if (conn != null) {
                  conn.rollback();
              }
          } catch (SQLException ex) {
              LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
          }
          LOG.warn("Encountered exception when processing partions of table. Just ignore partition or confirm if you are authorized to access table DBA_TAB_PARTITIONS.");
      }
      catch (Exception e) {
          LOG.error("Encountered exception when processing partions of table.");
      } finally {
          if (rs != null) {
              try {
                  rs.close();
              } catch (SQLException ex) {
                  LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
              }
          }
          if (pStmt != null) {
              try {
                  pStmt.close();
              } catch (SQLException ex) {
                  LoggingUtils.logAll(LOG, "Failed to close statement", ex);
              }
          }
// connection的关闭在manager用完后有manager.close()统一做
//          try {
//              close();
//          } catch (SQLException ex) {
//              LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
//          }
      }

      return partitionsList;
  }
}

