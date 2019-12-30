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
package com.creditease.dbus.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Abstract interface that manages connections to a database.
 */
public interface ConnManager {

    /**
     * @return the actual database connection.
     */
    Connection getConnection() throws Exception;

    /**
     * @return a string identifying the driver class to load for this
     * JDBC connection type.
     */
    String getDriverClass();

    /**
     * If a method of this ConnManager has returned a ResultSet to you,
     * you are responsible for calling release() after you close the
     * ResultSet object, to free internal resources. ConnManager
     * implementations do not guarantee the ability to have multiple
     * returned ResultSets available concurrently. Requesting a new
     * ResultSet from a ConnManager may cause other open ResulSets
     * to close.
     */
    void release() throws Exception;

    void close() throws Exception;

    /**
     * 使用connection 返回一个 statment用于后续使用，改statement 由lastStatement 管理
     *
     * @param sql
     * @return
     * @throws Exception
     */
    PreparedStatement prepareStatement(String sql) throws Exception;

}

