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
package com.creditease.dbus.common.format;

import java.io.IOException;


public abstract class InputSplit {
    protected String targetTableName;
    protected String tablePartitionInfo;

    // 查询数据库的collate，针对mysql。一般不指定。当用户在zk中配置指定collate时，查询数据的select 语句需要带上collate，以解决mysql大小写（缺省）不敏感问题，带来的数据拉重。
    // 通过将collate指定为utf_bin,可让mysql大小写敏感，从而解决数据重复问题，但性能下降，拉取速度慢。所以这个参数由用户指定。看其选择重复/快，还是不重复/慢的方式。下游wormhole具有幂等性，所以重复数据不会带来问题。
    protected String collate = ""; // query collate of db, apply to mysql. use collate general_ci, case insensitive; use collate utf_bin, case sensitive , and

    /**
     * Get the list of nodes by name where the data for the split would be local.
     * The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     * @throws IOException
     * @throws InterruptedException
     */
    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getCollate() {
        return " " + collate + " ";
    }

    public void setCollate(String collate) {
        this.collate = collate;
    }

    public String getTablePartitionInfo() {
        return tablePartitionInfo;
    }

    public void setTablePartitionInfo(String tablePartitionInfo) {
        this.tablePartitionInfo = tablePartitionInfo;
    }

}
