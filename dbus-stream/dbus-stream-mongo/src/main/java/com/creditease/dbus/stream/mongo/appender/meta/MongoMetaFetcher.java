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


package com.creditease.dbus.stream.mongo.appender.meta;

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.stream.common.appender.bean.TableComments;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Created by ximeiwang on 2017/12/21.
 */
public class MongoMetaFetcher implements MetaFetcher {

    private Logger logger = LoggerFactory.getLogger(MongoMetaFetcher.class);
    private DataSource ds;

    public MongoMetaFetcher(DataSource ds) throws Exception {
        this.ds = ds;
    }

    public DataSource getDataSource() {
        return ds;
    }

    @Override
    public MetaWrapper fetch2X(String schema, String tableName, int version) throws Exception {
        return null;
    }

    @Override
    public MetaWrapper fetch(String schema, String table, int version) throws Exception {
        return null;
    }

    @Override
    public TableComments fetchTableComments(String schema, String table) throws Exception {
        return null;
    }
}
