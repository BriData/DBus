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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.JdbcVo;
import com.google.common.base.Preconditions;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceContainer {

    private static DataSourceContainer container;

    private ConcurrentHashMap<String, BasicDataSource> cmap =
            new ConcurrentHashMap<String, BasicDataSource>();

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

    public boolean register(JdbcVo conf) {
        boolean isOk = true;
        try {
            BasicDataSource bds = new BasicDataSource();
            bds.setDriverClassName(conf.getDriverClass());
            bds.setUrl(conf.getUrl());
            bds.setUsername(conf.getUserName());
            bds.setPassword(conf.getPassword());
            bds.setInitialSize(conf.getInitialSize());
            bds.setMaxTotal(conf.getMaxActive());
            bds.setMaxIdle(conf.getMaxIdle());
            bds.setMinIdle(conf.getMinIdle());
            // 设置等待获取连接的最长时间
            // bds.setMaxWaitMillis(20 * 1000);
            // validQuery 只给test idle 使用，不给 test Borrow使用， 为什么？
            // 发现 oracle版本 insert心跳被block， 下面的link 有人说 可以通过设置TestOnBorrow=false 解决。
            // https://stackoverflow.com/questions/4853732/blocking-on-dbcp-connection-pool-open-and-close-connnection-is-database-conne
            bds.setTestOnBorrow(true);
            bds.setTestWhileIdle(false);

            if (StringUtils.equalsIgnoreCase(Constants.CONFIG_DB_TYPE_ORA, conf.getType())) {
                bds.setValidationQuery("select 1 from dual");
                bds.setValidationQueryTimeout(5);
            } else if (StringUtils.equalsIgnoreCase(Constants.CONFIG_DB_TYPE_MYSQL, conf.getType())) {
                bds.setValidationQuery("select 1");
                bds.setValidationQueryTimeout(5);
            }

            /*bds.setTimeBetweenEvictionRunsMillis(1000 * 300);
            if (org.apache.commons.lang.StringUtils.equals(Constants.CONFIG_DB_TYPE_ORA, conf.getType())) {
                bds.setValidationQuery("select 1 from dual");
                bds.setValidationQueryTimeout(1);
            } else if (org.apache.commons.lang.StringUtils.equals(Constants.CONFIG_DB_TYPE_MYSQL, conf.getType())) {
                bds.setValidationQuery("select 1");
                bds.setValidationQueryTimeout(1);
            }*/
            LoggerFactory.getLogger().info("create datasource key:" + conf.getKey() + " url:" + conf.getUrl());
            cmap.put(conf.getKey(), bds);

            // 为了支持查询主库和被库的延时，一个ds需要同时建立同主库和被库的连接
            if (conf instanceof DsVo) {
                DsVo ds = (DsVo) conf;
                if (StringUtils.isNotBlank(ds.getSlvaeUrl())) {
                    BasicDataSource slaveBds = new BasicDataSource();
                    slaveBds.setDriverClassName(ds.getDriverClass());
                    slaveBds.setUrl(ds.getSlvaeUrl());
                    slaveBds.setUsername(ds.getUserName());
                    slaveBds.setPassword(ds.getPassword());
                    slaveBds.setInitialSize(ds.getInitialSize());
                    slaveBds.setMaxTotal(ds.getMaxActive());
                    slaveBds.setMaxIdle(ds.getMaxIdle());
                    slaveBds.setMinIdle(ds.getMinIdle());
                    slaveBds.setTestOnBorrow(true);
                    slaveBds.setTestWhileIdle(false);

                    if (StringUtils.equalsIgnoreCase(Constants.CONFIG_DB_TYPE_ORA, conf.getType())) {
                        slaveBds.setValidationQuery("select 1 from dual");
                        slaveBds.setValidationQueryTimeout(5);
                    } else if (StringUtils.equalsIgnoreCase(Constants.CONFIG_DB_TYPE_MYSQL, conf.getType())) {
                        slaveBds.setValidationQuery("select 1");
                        slaveBds.setValidationQueryTimeout(5);
                    }

                    // 设置等待获取连接的最长时间
                    // slaveBds.setMaxWaitMillis(20 * 1000);
                    String key = StringUtils.join(new String[]{ds.getKey(), "slave"}, "_");
                    LoggerFactory.getLogger().info("create datasource key:" + key + " url:" + ds.getSlvaeUrl());
                    cmap.put(key, slaveBds);
                } else {
                    LoggerFactory.getLogger().warn("db container initDsPool key " + ds.getKey() + " of slave url is empty.");
                }
            }

        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db container initDsPool key " + conf.getKey() + " datasource error!]", e);
            isOk = false;
        }
        return isOk;
    }

    public Connection getConn(String key) {
        Connection conn = null;
        try {
            if (cmap.containsKey(key)) {
                LoggerFactory.getLogger().info("db container get key: " + key + " conn start");
                conn = cmap.get(key).getConnection();
                LoggerFactory.getLogger().info("db container get key: " + key + " conn end");
            }
        } catch (SQLException e) {
            LoggerFactory.getLogger().error("[db container get key: " + key + " conn error!]", e);
        }
        Preconditions.checkNotNull(conn, "db container get key: " + key + " conn error!");
        return conn;
    }

    public void release(String key) {
        try {
            if (cmap.containsKey(key))
                cmap.get(key).close();
        } catch (SQLException e) {
            LoggerFactory.getLogger().error("[db container close key: " + key + " datasource error!]", e);
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
