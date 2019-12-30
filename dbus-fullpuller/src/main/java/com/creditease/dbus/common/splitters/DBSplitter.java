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

import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.exception.ValidationException;
import com.creditease.dbus.common.format.InputSplit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


/**
 * DBSplitter will generate DBInputSplits to use with DataDBInputFormat.
 * DataDBInputFormat needs to interpolate between two values that
 * represent the lowest and highest valued records to import. Depending
 * on the data-type of the column, this requires different behavior.
 * DBSplitter implementations should perform this for a data type or family
 * of data types.
 */
public interface DBSplitter {

    /**
     * Given a ResultSet containing one record (and already advanced to that
     * record) with two columns (a low value, and a high value, both of the same
     * type), determine a set of splits that span the given values.
     *
     * @param numSplits TODO
     * @param dbConf    TODO
     */
    List<InputSplit> split(long numSplits, ResultSet results, String colName, DBConfiguration dbConf)
            throws SQLException, ValidationException;
}
