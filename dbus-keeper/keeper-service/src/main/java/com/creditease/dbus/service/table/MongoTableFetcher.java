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

import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.TableMeta;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MongoTableFetcher{
    private DataSource ds;
    public MongoTableFetcher(DataSource ds) {
        this.ds = ds;
    }

    public List<DataTable> fetchTable(String schemaName) throws Exception {

        MongoClient client = null;
        List<DataTable> list;
        try {
            String url = ds.getMasterUrl();
            String user = ds.getDbusUser();
            String password = ds.getDbusPwd();
            MongoCredential credential = MongoCredential.createCredential(user, "admin", password.toCharArray());
            ArrayList<ServerAddress> serverAddresses = new ArrayList<>();
            for (String urlOne : url.split(",")) {
                String[] host_port = urlOne.split(":");
                serverAddresses.add(new ServerAddress(host_port[0], Integer.parseInt(host_port[1])));
            }
            client = new MongoClient(serverAddresses, Arrays.asList(credential));
            MongoDatabase database = client.getDatabase(schemaName);

            list = new ArrayList<>();
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
        } finally {
            if (client != null) {
                client.close();
            }
        }
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
