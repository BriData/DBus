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

package com.creditease.dbus.heartbeat.container;

import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.mongo.DBusMongoClient;
import com.creditease.dbus.heartbeat.vo.JdbcVo;
import com.mongodb.MongoClient;

public class MongoClientContainer {

    private static MongoClientContainer container;

    private ConcurrentHashMap<String, DBusMongoClient> cmap = new ConcurrentHashMap<>();

    private MongoClientContainer() {
    }

    public static MongoClientContainer getInstance() {
        if (container == null) {
            synchronized (MongoClientContainer.class) {
                if (container == null)
                    container = new MongoClientContainer();
            }
        }
        return container;
    }

    public boolean register(JdbcVo conf) {
        boolean isOk = true;
        try {
            cmap.put(conf.getKey(), new DBusMongoClient(conf));
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[mongo client container init error. key " + conf.getKey(), e);
            isOk = false;
        }
        return isOk;
    }

    public DBusMongoClient getMongoClient(String key) {
        DBusMongoClient client = null;
        if (cmap.containsKey(key))
            client = cmap.get(key);
        return client;
    }

    public void release(String key) {
        if (cmap.containsKey(key)) {
            DBusMongoClient client = cmap.get(key);
            client.getMongoClient().close();
            if (client.getShardMongoClients() != null &&
                client.getShardMongoClients().size() > 0) {
                for (Map.Entry<String, MongoClient> mc : client.getShardMongoClients().entrySet())
                    mc.getValue().close();
            }
        }
    }

    public void clear() {
        Enumeration<String> keys = cmap.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            release(key);
        }
        cmap.clear();
    }
}
