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

package com.creditease.dbus.service.schema;

import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoSchemaFetcher{
    private DataSource ds;
    public MongoSchemaFetcher(DataSource ds) {
        this.ds = ds;
    }


    public List<DataSchema> fetchSchema() {
        MongoClient client = null;
        List<DataSchema> list;
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

            list = new ArrayList<>();
            DataSchema schema;

            for (String name : client.listDatabaseNames()) {
                schema = new DataSchema();
                schema.setSchemaName(name);
                schema.setStatus("active");
                list.add(schema);
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return list;
    }

    public static void main(String[] args) {

    }
}
