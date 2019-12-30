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
package com.creditease.dbus.common.splitters;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.format.InputSplit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
            return splits;
        }

        boolean minVal = results.getBoolean(1);
        boolean maxVal = results.getBoolean(2);

        // Use one or two splits.
        if (!minVal) {
            splits.add(new DataDBInputSplit(type, colName, " = ", false, " = ", false));
        }

        if (maxVal) {
            splits.add(new DataDBInputSplit(type, colName, " = ", true, " = ", true));
        }

        if (results.getString(1) == null || results.getString(2) == null) {
            // Include a null value.
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
        }

        return splits;
    }
}
