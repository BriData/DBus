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

package com.creditease.dbus.service.table;

import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.TableMeta;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoTableFetcher{
    private DataSource ds;
    public MongoTableFetcher(DataSource ds) {
        this.ds = ds;
    }

    public List<DataTable> fetchTable(String schemaName) throws Exception {
        MongoClientURI uri = new MongoClientURI(ds.getMasterUrl());
        MongoClient client = new MongoClient(uri);
        MongoDatabase database = client.getDatabase(schemaName);


        List<DataTable> list = new ArrayList<>();
        DataTable table;

        for (String name : database.listCollectionNames()) {
            table = new DataTable();
            table.setTableName(name);
            table.setPhysicalTableRegex(name);
            table.setVerId(null);
            table.setStatus("ok");
            table.setCreateTime(new java.util.Date());
            list.add(table);
        }

        client.close();

        return list;
    }

    public List<List<TableMeta>> fetchTableField(Map<String, Object> params, List<Map<String, Object>> paramsList){
        List<List<TableMeta>> ret = new ArrayList<>();
        for (Map<String, Object> map : paramsList) {
            ret.add(new ArrayList<>());
        }
        return ret;
    }
}
