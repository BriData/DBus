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
package com.creditease.dbus.common.splitters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.InputSplit;

/**
 * Fix bug by Dbus team 20161230
 * Implement DBSplitter over floating-point values.
 */
public class FloatSplitter implements DBSplitter  {

  private Logger LOG = LoggerFactory.getLogger(getClass());

  private static final double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

  //private final int type = Types.FLOAT;
  private final int type = Types.DOUBLE;

  public List<InputSplit> split(long numSplits, ResultSet results,
      String colName, DBConfiguration dbConf) throws SQLException {

    LOG.warn("Generating splits for a floating-point index column. Due to the");
    LOG.warn("imprecise representation of floating-point values in Java, this");
    LOG.warn("may result in an incomplete import.");
    LOG.warn("You are strongly encouraged to choose an integral split column.");

    List<InputSplit> splits = new ArrayList<InputSplit>();

    if (results.getString(1) == null && results.getString(2) == null) {
      // Range is null to null. Return a null split accordingly.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, DataPullConstants.QUERY_COND_IS_NULL, null, DataPullConstants.QUERY_COND_IS_NULL, null));
      return splits;
    }

    double minVal = results.getDouble(1);
    double maxVal = results.getDouble(2);

    // Use this as a hint. May need an extra task if the size doesn't
    // divide cleanly.
    double splitSize = (maxVal - minVal) / (double) numSplits;

    if (splitSize < MIN_INCREMENT) {
      splitSize = MIN_INCREMENT;
    }

    double curLower = minVal;
    double curUpper = curLower + splitSize;

    while (curUpper < maxVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, " >= ", curLower, " < ", curUpper));

      curLower = curUpper;
      curUpper += splitSize;
    }

    // Catch any overage and create the closed interval for the last split.
    if (curLower <= maxVal || splits.size() == 1) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, " >= ", curUpper, " <= ", maxVal));
    }

    if (results.getString(1) == null || results.getString(2) == null) {
      // At least one extrema is null; add a null split.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, DataPullConstants.QUERY_COND_IS_NULL, null, DataPullConstants.QUERY_COND_IS_NULL, null));
    }

    return splits;
  }
}
