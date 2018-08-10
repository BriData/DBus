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

package com.creditease.dbus.bean;

import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataTable;

import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-06-06
 * Time: 下午4:35
 */
public class SchemaAndTablesInfoBean {
    private DataSchema schema;
    private List<DataTable> tables;

    public DataSchema getSchema() {
        return schema;
    }

    public void setSchema(DataSchema schema) {
        this.schema = schema;
    }

    public List<DataTable> getTables() {
        return tables;
    }

    public void setTables(List<DataTable> tables) {
        this.tables = tables;
    }
}
