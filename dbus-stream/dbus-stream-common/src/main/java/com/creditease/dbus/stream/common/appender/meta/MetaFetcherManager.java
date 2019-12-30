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


package com.creditease.dbus.stream.common.appender.meta;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.exception.UnintializedException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DbusDatasource;
import com.creditease.dbus.stream.common.appender.db.DataSourceProvider;
import com.creditease.dbus.stream.common.appender.db.DruidDataSourceProvider;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Shrimp on 16/5/17.
 */
public class MetaFetcherManager {
    private static Logger logger = LoggerFactory.getLogger(MetaFetcherManager.class);
    private static final String ORA_DATASOURCE_CONFIG_NAME = Constants.Properties.ORA_META;

    private static final String DB2_DATASOURCE_CONFIG_NAME = Constants.Properties.DB2_META;

    private static MetaFetcher fetcher;
    private static AtomicBoolean initialized = new AtomicBoolean(false);

    private static boolean initializeOraMetaFetcher(String jdbcUrl, String user, String pwd) {
        try {
            Properties properties = PropertiesHolder.getProperties(ORA_DATASOURCE_CONFIG_NAME);
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", user);
            properties.setProperty("password", pwd);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            Class<?> clazz = Class.forName("com.creditease.dbus.stream.oracle.appender.meta.OraMetaFetcher");
            Constructor<?> constructor = clazz.getConstructor(DataSource.class);
            fetcher = (MetaFetcher) constructor.newInstance(provider.provideDataSource());
            return true;
        } catch (Exception e) {
            logger.error("MetaFetcherManager initializeOraMetaFetcher error!", e);
            return false;
        }
    }

    public static MetaFetcher getOraMetaFetcher() {
        if (!initialized.get()) {
            synchronized (MetaFetcherManager.class) {
                DbusDatasource ds = Utils.getDatasource();
                boolean result = initializeOraMetaFetcher(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
                if (!result) {
                    result = initializeOraMetaFetcher(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
                    if (!result) {
                        throw new UnintializedException("DBFacadeManager initialized error!");
                    }
                }
                initialized.set(true);
            }
        }
        return fetcher;
    }

    private static boolean initializeDb2MetaFetcher(String jdbcUrl, String user, String pwd) {
        try {
            Properties properties = PropertiesHolder.getProperties(DB2_DATASOURCE_CONFIG_NAME);
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", user);
            properties.setProperty("password", pwd);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            Class<?> clazz = Class.forName("com.creditease.dbus.stream.db2.appender.meta.Db2MetaFetcher");
            Constructor<?> constructor = clazz.getConstructor(DataSource.class);
            fetcher = (MetaFetcher) constructor.newInstance(provider.provideDataSource());
            return true;
        } catch (Exception e) {
            logger.error("MetaFetcherManager initializeOraMetaFetcher error!", e);
            return false;
        }
    }

    public static MetaFetcher getDb2MetaFetcher() {
        if (!initialized.get()) {
            synchronized (MetaFetcherManager.class) {
                DbusDatasource ds = Utils.getDatasource();
                boolean result = initializeDb2MetaFetcher(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
                if (!result) {
                    result = initializeDb2MetaFetcher(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
                    if (!result) {
                        throw new UnintializedException("DBFacadeManager initialized error!");
                    }
                }
                initialized.set(true);
            }
        }
        return fetcher;
    }


    private static boolean initializeMysqlMetaFetcher(String jdbcUrl, String user, String pwd) {
        try {
            Properties properties = new Properties();
            properties.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", user);
            properties.setProperty("password", pwd);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            Class<?> clazz = Class.forName("com.creditease.dbus.stream.mysql.appender.meta.MysqlMetaFetcher");
            Constructor<?> constructor = clazz.getConstructor(DataSource.class);
            fetcher = (MetaFetcher) constructor.newInstance(provider.provideDataSource());
            return true;
        } catch (Exception e) {
            logger.error("MetaFetcherManager initializeMysqlMetaFetcher error!", e);
            return false;
        }
    }

    public static MetaFetcher getMysqlMetaFetcher() {
        if (!initialized.get()) {
            synchronized (MetaFetcherManager.class) {
                DbusDatasource ds = Utils.getDatasource();
                boolean result = initializeMysqlMetaFetcher(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
                if (!result) {
                    result = initializeMysqlMetaFetcher(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
                    if (!result) {
                        throw new UnintializedException("DBFacadeManager initialized error!");
                    }
                }
                initialized.set(true);
            }
        }
        return fetcher;
    }
/*
    private static boolean initializeMongoMetaFetcher(String jdbcUrl, String user, String pwd) {
        try {
            Properties properties = new Properties();
            properties.setProperty("driverClassName", "com.mongo.jdbc.Driver");//TODO 不知MongoDB的driver是否是这个格式呢
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", user);
            properties.setProperty("password", pwd);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            Class<?> clazz = Class.forName("com.creditease.dbus.stream.mongo.appender.meta.MongoMetaFetcher");
            Constructor<?> constructor = clazz.getConstructor(DataSource.class);
            fetcher = (MetaFetcher) constructor.newInstance(provider.provideDataSource());
            return true;
        } catch (Exception e) {
            logger.error("MetaFetcherManager initializeMongoMetaFetcher error!", e);
            return false;
        }
    }

    public static MetaFetcher getMongoMetaFetcher() {
        if (!initialized.get()) {
            synchronized (MetaFetcherManager.class) {
                DbusDatasource ds = Utils.getDatasource();
                boolean result = initializeMongoMetaFetcher(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
                if (!result) {
                    result = initializeMongoMetaFetcher(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
                    if (!result) {
                        throw new UnintializedException("DBFacadeManager initialized error!");
                    }
                }
                initialized.set(true);
            }
        }
        return fetcher;
    }
*/

    public static void reset() {
        initialized.compareAndSet(true, false);
    }
}
