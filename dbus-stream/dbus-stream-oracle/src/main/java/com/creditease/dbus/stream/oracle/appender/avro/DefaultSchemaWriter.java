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


package com.creditease.dbus.stream.oracle.appender.avro;

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.AvroSchema;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 将Schema信息保存到 memory/mysql
 * Created by Shrimp on 16/5/18.
 */
public class DefaultSchemaWriter implements DBusSchemaWriter {
    private Logger logger = LoggerFactory.getLogger(DefaultSchemaWriter.class);
    private Executor executor;
    private boolean needPersistent = false;

    public DefaultSchemaWriter(boolean needPersistent) {
        this.needPersistent = needPersistent;
        if (needPersistent) {
            executor = Executors.newFixedThreadPool(1);
        }
    }

    @Override
    public void write(final String schema, Object... args) throws Exception {
        final String namespace = (String) args[0];
        final String schemaName = (String) args[1];
        final int schemaHash = (int) args[2];

        String key = Utils.buildAvroSchemaName(schemaHash, namespace, schemaName);
        ThreadLocalCache.put(Constants.CacheNames.AVRO_SCHEMA_CACHE, key, schema);
        logger.info("write avro schema to cache completely:[key:" + key + "]");
        if (needPersistent) {
            executor.execute(() -> {
                AvroSchema as = new AvroSchema();
                as.setDsId(Utils.getDatasource().getId());
                as.setNamespace(namespace);
                as.setSchemaName(schemaName);
                as.setFullName(key);
                as.setSchema(schema);
                as.setSchemaHash(schemaHash);
                as.setTs(new Timestamp(System.currentTimeMillis()));
                try {
                    DBFacadeManager.getDbFacade().saveAvroSchema(as);
                    logger.info("save avro schema to database completely");
                } catch (Exception e) {
                    logger.error("Save AvroSchema Error", e);
                }
            });
        }
    }
}
