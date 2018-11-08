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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.creditease.dbus.common.utils.DBConfiguration;

/**
 * Database manager that queries catalog tables directly
 * (instead of metadata calls) to retrieve information.
 */
public abstract class CatalogQueryManager
    extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(
    CatalogQueryManager.class.getName());

  public CatalogQueryManager(final String driverClass,
    final DBConfiguration opts, String conString) {
    super(driverClass, opts, conString);
  }

  protected abstract String getListDatabasesQuery();
  @Override
  public String[] listDatabases() {
    Connection c = null;
    Statement s = null;
    ResultSet rs = null;
    List<String> databases = new ArrayList<String>();
    try {
      c = getConnection();
      s = c.createStatement();
      rs = s.executeQuery(getListDatabasesQuery());
      while (rs.next()) {
        databases.add(rs.getString(1));
      }
      c.commit();
    } catch (SQLException sqle) {
      try {
        if (c != null) {
          c.rollback();
        }
      } catch (SQLException ce) {
          LOG.warn("Failed to rollback transaction", ce);
      }
      LOG.warn("Failed to list databases", sqle);
      throw new RuntimeException(sqle);
    }catch (Exception e) {
        // TODO Auto-generated catch block
        LOG.error(e.getMessage(),e);
        return null;
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException re) {
          LOG.warn("Failed to close resultset", re);
        }
      }
      if (s != null) {
        try {
          s.close();
        } catch (SQLException se) {
          LOG.warn("Failed to close statement", se);
        }
      }
    }

    return databases.toArray(new String[databases.size()]);
  }

  protected abstract String getListTablesQuery();
  @Override
  public String[] listTables() {
    Connection c = null;
    Statement s = null;
    ResultSet rs = null;
    List<String> tables = new ArrayList<String>();
    try {
      c = getConnection();
      s = c.createStatement();
      rs = s.executeQuery(getListTablesQuery());
      while (rs.next()) {
        tables.add(rs.getString(1));
      }
      c.commit();
    } catch (SQLException sqle) {
      try {
        if (c != null) {
          c.rollback();
        }
      } catch (SQLException ce) {
        LOG.warn("Failed to rollback transaction", ce);
      }
      LOG.warn("Failed to list tables", sqle);
      throw new RuntimeException(sqle);
    }catch (Exception e) {
        // TODO Auto-generated catch block
        LOG.error(e.getMessage(),e);
        return null;
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException re) {
          LOG.warn("Failed to close resultset", re);
        }
      }
      if (s != null) {
        try {
          s.close();
        } catch (SQLException se) {
          LOG.warn("Failed to close statement", se);
        }
      }
    }

    return tables.toArray(new String[tables.size()]);
  }

  protected abstract String getListColumnsQuery(String tableName);
  @Override
  public String[] getColumnNames(String tableName) {
    Connection c = null;
    Statement s = null;
    ResultSet rs = null;
    List<String> columns = new ArrayList<String>();
    String listColumnsQuery = getListColumnsQuery(tableName);
    try {
      c = getConnection();
      s = c.createStatement();
      rs = s.executeQuery(listColumnsQuery);
      while (rs.next()) {
        columns.add(rs.getString(1));
      }
      c.commit();
    } catch (SQLException sqle) {
      try {
        if (c != null) {
          c.rollback();
        }
      } catch (SQLException ce) {
        LOG.warn("Failed to rollback transaction", ce);
      }
      LOG.warn("Failed to list columns from query: "
        + listColumnsQuery, sqle);
      throw new RuntimeException(sqle);
    }catch (Exception e) {
        // TODO Auto-generated catch block
        LOG.error(e.getMessage(),e);
        return null;
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException re) {
          LOG.warn("Failed to close resultset", re);
        }
      }
      if (s != null) {
        try {
          s.close();
        } catch (SQLException se) {
          LOG.warn("Failed to close statement", se);
        }
      }
    }

    return columns.toArray(new String[columns.size()]);
  }

  protected abstract String getPrimaryKeyQuery(String tableName);
  @Override
  public String getPrimaryKey(String tableName) {
    Connection c = null;
    Statement s = null;
    ResultSet rs = null;
    List<String> columns = new ArrayList<String>();
    try {
      c = getConnection();
      s = c.createStatement();

      String primaryKeyQuery = getPrimaryKeyQuery(tableName);
      LOG.debug("Retrieving primary key for table '"
        + tableName + "' with query " + primaryKeyQuery);
      rs = s.executeQuery(primaryKeyQuery);
      while (rs.next()) {
        columns.add(rs.getString(1));
      }
      c.commit();
    } catch (SQLException sqle) {
      try {
        if (c != null) {
          c.rollback();
        }
      } catch (SQLException ce) {
        LOG.warn("Failed to rollback transaction", ce);
      }
      LOG.warn("Failed to list primary key", sqle);
      throw new RuntimeException(sqle);
    }catch (Exception e) {
        // TODO Auto-generated catch block
        LOG.error(e.getMessage(),e);
        return null;
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException re) {
          LOG.warn("Failed to close resultset", re);
        }
      }
      if (s != null) {
        try {
          s.close();
        } catch (SQLException se) {
          LOG.warn("Failed to close statement", se);
        }
      }
    }

    if (columns.size() == 0) {
      // Table has no primary key
      return null;
    }

    if (columns.size() > 1) {
      // The primary key is multi-column primary key. Warn the user.
      LOG.warn("The table " + tableName + " "
        + "contains a multi-column primary key. Sqoop will default to "
        + "the column " + columns.get(0) + " only for this job.");
    }

    return columns.get(0);
  }
}

