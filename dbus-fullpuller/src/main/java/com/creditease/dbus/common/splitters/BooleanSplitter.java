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

import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.InputSplit;

/**
 * Fix bug by Dbus team 20161230
 * Implement DBSplitter over boolean values.
 */
public class BooleanSplitter implements DBSplitter {
  private final int type = Types.BOOLEAN;

  public List<InputSplit> split(long numSplits, ResultSet results,
      String colName, DBConfiguration dbConf) throws SQLException {

    List<InputSplit> splits = new ArrayList<InputSplit>();

    if (results.getString(1) == null && results.getString(2) == null) {
      // Range is null to null. Return a null split accordingly.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, DataPullConstants.QUERY_COND_IS_NULL, null, DataPullConstants.QUERY_COND_IS_NULL, null));
      return splits;
    }

    boolean minVal = results.getBoolean(1);
    boolean maxVal = results.getBoolean(2);

    // Use one or two splits.
    if (!minVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, " = ", false, " = ", false));
    }

    if (maxVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, " = ", true, " = ", true));
    }

    if (results.getString(1) == null || results.getString(2) == null) {
      // Include a null value.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              type, colName, DataPullConstants.QUERY_COND_IS_NULL, null, DataPullConstants.QUERY_COND_IS_NULL, null));
    }

    return splits;
  }
}
