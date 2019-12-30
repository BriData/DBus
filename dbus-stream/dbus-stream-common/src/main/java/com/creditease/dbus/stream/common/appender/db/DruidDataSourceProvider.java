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


package com.creditease.dbus.stream.common.appender.db;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * 根据配置文件生成Druid DataSource
 * Created by Shrimp on 16/5/17.
 */
public class DruidDataSourceProvider implements DataSourceProvider {
    private DataSource ds;

    public DruidDataSourceProvider(Properties properties) throws Exception {
        ds = DruidDataSourceFactory.createDataSource(properties);
    }

    @Override
    public DataSource provideDataSource() throws Exception {
        return ds;
    }
}
