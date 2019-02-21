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

package com.creditease.dbus.service.source;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class MongoSourceFetcher {

    public int fetchSource(Map<String, Object> map) {
        int ret;
        MongoClient client = null;
        try {
            String url = map.get("URL").toString();
            String user = map.get("user").toString();
            String password = map.get("password").toString();
            MongoCredential credential = MongoCredential.createCredential(user, "admin", password.toCharArray());
            ArrayList<ServerAddress> serverAddresses = new ArrayList<>();
            for (String urlOne : url.split(",")) {
                String[] host_port = urlOne.split(":");
                serverAddresses.add(new ServerAddress(host_port[0], Integer.parseInt(host_port[1])));
            }
            client = new MongoClient(serverAddresses, Arrays.asList(credential));

            ret = 0;
            for (String name : client.listDatabaseNames()) {
                ret = 1;
                break;
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return ret;
    }
}
