/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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
import com.creditease.dbus.stream.common.appender.db.DataSourceProvider;
import com.creditease.dbus.stream.common.appender.db.DruidDataSourceProvider;
import com.creditease.dbus.stream.common.appender.bean.DbusDatasource;
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
    private static final String DATASOURCE_CONFIG_NAME = Constants.Properties.ORA_META;
    private static MetaFetcher fetcher;
    private static AtomicBoolean initialized = new AtomicBoolean(false);

    private static boolean initialize(String jdbcUrl, String user, String pwd) {
        try {
            Properties properties = PropertiesHolder.getProperties(DATASOURCE_CONFIG_NAME);
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", user);
            properties.setProperty("password", pwd);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            Class<?> clazz = Class.forName("com.creditease.dbus.stream.oracle.appender.meta.OraMetaFetcher");
            Constructor<?> constructor = clazz.getConstructor(DataSource.class);
            fetcher = (MetaFetcher) constructor.newInstance(provider.provideDataSource());
            return true;
        } catch (Exception e) {
            logger.error("MetaFetcherManager initialize error!", e);
            return false;
        }
    }

    public static MetaFetcher getFetcher() {
        if (!initialized.get()) {
            synchronized (MetaFetcherManager.class) {
                DbusDatasource ds = Utils.getDatasource();
                boolean result = initialize(ds.getMasterUrl(), ds.getDbusUser(), ds.getDbusPwd());
                if (!result) {
                    result = initialize(ds.getSlaveUrl(), ds.getDbusUser(), ds.getDbusPwd());
                    if (!result) {
                        throw new UnintializedException("DBFacadeManager initialized error!");
                    }
                }
                initialized.set(true);
            }
        }
        return fetcher;
    }

    public static void reset() {
        initialized.compareAndSet(true, false);
    }
}
