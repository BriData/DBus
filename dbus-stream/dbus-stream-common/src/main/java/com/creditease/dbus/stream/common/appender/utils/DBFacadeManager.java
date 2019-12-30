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


package com.creditease.dbus.stream.common.appender.utils;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.exception.UnintializedException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.db.DataSourceProvider;
import com.creditease.dbus.stream.common.appender.db.DruidDataSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Shrimp on 16/5/17.
 */
public class DBFacadeManager {
    private static Logger logger = LoggerFactory.getLogger(DBFacadeManager.class);
    private static final String DATASOURCE_CONFIG_NAME = Constants.ZKPath.ZK_COMMONS + "/" + Constants.Properties.MYSQL;
    private static DBFacade facade;
    private static AtomicBoolean initialized = new AtomicBoolean(false);

    private static void initialize() {
        try {
            Properties properties = PropertiesHolder.getProperties(DATASOURCE_CONFIG_NAME);
            DataSourceProvider provider = new DruidDataSourceProvider(properties);
            facade = new DBFacade(provider.provideDataSource());
        } catch (Exception e) {
            logger.error("DBFacadeManager initialize error", e);
            throw new UnintializedException("DBFacadeManager initialize error", e);
        }
    }

    public static DBFacade getDbFacade() {
        if (!initialized.get()) {
            synchronized (DBFacadeManager.class) {
                if (!initialized.get()) {
                    initialize();
                    logger.info("DBFacadeManager initialized");
                }
            }
            initialized.set(true);
        }

        return facade;
    }
}
