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

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using java.utils.HashMap to store primitive data types such as int can
 * be unsafe because auto-unboxing a null value in the map can cause a NPE.
 *
 * SqlTypeMap is meant to be safer because it provides validation for arguments
 * and fails fast with informative messages if invalid arguments are given.
 */
public class SqlTypeMap<K, V> extends HashMap<K, V> {

  private static final long serialVersionUID = 1L;

  private Logger LOG = LoggerFactory.getLogger(getClass());

  @Override
  public V get(Object col) {
    V sqlType = super.get(col);
    if (sqlType == null) {
      LOG.error("It seems like you are looking up a column that does not");
      LOG.error("exist in the table. Please ensure that you've specified");
      LOG.error("correct column names in options.");
      throw new IllegalArgumentException("column not found: " + col);
    }
    return sqlType;
  }

  @Override
  public V put(K col, V sqlType) {
    if (sqlType == null) {
      throw new IllegalArgumentException("sql type cannot be null");
    }
    return super.put(col, sqlType);
  }
}
