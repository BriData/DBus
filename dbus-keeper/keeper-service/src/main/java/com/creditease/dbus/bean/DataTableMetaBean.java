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


package com.creditease.dbus.bean;

import com.creditease.dbus.domain.model.DdlEvent;
import com.creditease.dbus.domain.model.TableMeta;

import java.util.List;

public class DataTableMetaBean {
    DataTableBean dataTable;
    List<TableMeta> tableMetas;
    List<DdlEvent> ddlEvents;

    public DataTableBean getDataTable() {
        return dataTable;
    }

    public void setDataTable(DataTableBean dataTable) {
        this.dataTable = dataTable;
    }

    public List<TableMeta> getTableMetas() {
        return tableMetas;
    }

    public void setTableMetas(List<TableMeta> tableMetas) {
        this.tableMetas = tableMetas;
    }

    public List<DdlEvent> getDdlEvents() {
        return ddlEvents;
    }

    public void setDdlEvents(List<DdlEvent> ddlEvents) {
        this.ddlEvents = ddlEvents;
    }
}
