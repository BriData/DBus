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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.creditease.dbus.common.utils.DBConfiguration;

/**
 * Database manager that queries "information schema" directly
 * (instead of metadata calls) to retrieve information.
 */
public abstract class InformationSchemaManager
    extends CatalogQueryManager {

  public static final Log LOG = LogFactory.getLog(
    InformationSchemaManager.class.getName());

  public InformationSchemaManager(final String driverClass,
    final DBConfiguration opts, String conString) {
    super(driverClass, opts, conString);
  }

  protected abstract String getSchemaQuery();

  @Override
  protected String getListTablesQuery() {
    return
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
    + "WHERE TABLE_SCHEMA = (" + getSchemaQuery() + ")";
  }

  @Override
  protected String getListColumnsQuery(String tableName) {
    return
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
    + "WHERE TABLE_SCHEMA = (" + getSchemaQuery() + ") "
    + "  AND TABLE_NAME = '" + tableName + "' ";
  }

  @Override
  protected String getPrimaryKeyQuery(String tableName) {
    return
      "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc, "
    + "  INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
    + "WHERE tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
    + "  AND tc.TABLE_NAME = kcu.TABLE_NAME "
    + "  AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
    + "  AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
    + "  AND tc.TABLE_SCHEMA = (" + getSchemaQuery() + ") "
    + "  AND tc.TABLE_NAME = '" + tableName + "' "
    + "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";
  }
}

