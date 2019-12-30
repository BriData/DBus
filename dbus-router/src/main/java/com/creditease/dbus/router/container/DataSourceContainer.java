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


package com.creditease.dbus.router.container;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceContainer {

    private static Logger logger = LoggerFactory.getLogger(DataSourceContainer.class);

    private static DataSourceContainer container;

    private ConcurrentHashMap<String, DataSource> cmap = new ConcurrentHashMap();

    private DataSourceContainer() {
    }

    public static DataSourceContainer getInstance() {
        if (container == null) {
            synchronized (DataSourceContainer.class) {
                if (container == null)
                    container = new DataSourceContainer();
            }
        }
        return container;
    }

    public synchronized boolean register(String key, Properties properties) {
        boolean isOk = true;
        try {
            if (!cmap.containsKey(key))
                cmap.put(key, DruidDataSourceFactory.createDataSource(properties));
        } catch (Exception e) {
            logger.error("db container init key " + key + " datasource error!", e);
            isOk = false;
        }
        return isOk;
    }

    public Connection getConn(String key) {
        Connection conn = null;
        try {
            if (cmap.containsKey(key))
                conn = cmap.get(key).getConnection();
        } catch (SQLException e) {
            logger.error("db container get key: " + key + " conn error!", e);
        }
        return conn;
    }

    private void release(String key) {
        if (cmap.containsKey(key))
            ((DruidDataSource) cmap.get(key)).close();
    }

    public synchronized void clear() {
        Enumeration<String> keys = cmap.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            release(key);
        }
        cmap.clear();
    }
}
