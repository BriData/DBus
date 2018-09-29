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

package com.creditease.dbus.stream.oracle.appender.avro;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.Constants.ConfigureKey;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.bean.AvroSchema;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.creditease.dbus.stream.common.Constants.Properties.CONFIGURE;

/**
 * Created by Shrimp on 16/5/11.
 */
public class SchemaProvider {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ConcurrentMap<String, Schema> schemaCache;
    private static SchemaProvider instance = new SchemaProvider();

    private SchemaProvider() {
        schemaCache = new ConcurrentHashMap<>();
    }

    public static SchemaProvider getInstance() {
        return instance;
    }

    /**
     * 删除缓存的avro schema
     *
     * @param schemaName
     */
    public void remove(String schemaName) {
        ThreadLocalCache.remove(Constants.CacheNames.AVRO_SCHEMA_CACHE, schemaName);
        schemaCache.remove(schemaName);
    }

    public Schema getSchema(String schemaName) throws Exception {
        Schema schema = schemaCache.get(schemaName);
        if (schema != null) {
            return schema;
        }

        schema = new Schema.Parser().parse(getSchemaAsString(schemaName));
        schemaCache.putIfAbsent(schemaName, schema);

        return schema;
    }

    public String getSchemaAsString(String schemaName) {

        String schemaStr = null;
        int i = 0;

        // 等待需要的schema信息
        while (schemaStr == null) {
            try {
                schemaStr = ThreadLocalCache.get(Constants.CacheNames.AVRO_SCHEMA_CACHE, schemaName);
                if (schemaStr == null) {
                    AvroSchema bean = DBFacadeManager.getDbFacade().queryAvroSchema(schemaName, Utils.getDatasource().getId());
                    if (bean != null) {
                        logger.info("query schema[" + schemaName + "] from database");
                        schemaStr = bean.getSchema();
                        ThreadLocalCache.put(Constants.CacheNames.AVRO_SCHEMA_CACHE, schemaName, schemaStr);
                    }
                }

                try {
                    if (schemaStr == null) {
                        String fullDataSchema = PropertiesHolder.getProperties(CONFIGURE, ConfigureKey.FULLDATA_REQUEST_SRC);
                        String heartbeatSchema = PropertiesHolder.getProperties(CONFIGURE, ConfigureKey.HEARTBEAT_SRC);
                        String metaEventSchema = PropertiesHolder.getProperties(CONFIGURE, ConfigureKey.META_EVENT_SRC);
                        if (Constants.GENERIC_SCHEMA.equals(schemaName)) {
                            schemaStr = initializeLocalSchema(schemaName);
                        } else if (schemaName.contains(fullDataSchema)) {
                            schemaStr = initializeLocalSchema(fullDataSchema);
                        } else if (schemaName.contains(heartbeatSchema)) {
                            schemaStr = initializeLocalSchema(heartbeatSchema);
                        } else if (schemaName.contains(metaEventSchema)) {
                            schemaStr = initializeLocalSchema(metaEventSchema);
                        }
                        if (schemaStr != null) {
                            ThreadLocalCache.put(Constants.CacheNames.AVRO_SCHEMA_CACHE, schemaName, schemaStr);
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Initialize avro schema[{}] error!", schemaName, e);
                }

                logger.debug("[" + (++i) + "]waiting for Avro Schema:" + schemaName);
                if (i > 100) break;
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (Exception e) {
                logger.error("Get avro schema string error", e);
                throw new IllegalStateException(e);
            }
        }
        // 如果等待
        if (schemaStr == null) {
            schemaStr = generateAvroSchema(schemaName);
            if (!schemaName.endsWith("." + schemaStr.hashCode())) {
                logger.warn("Hash code[{}] of Generated avro schema is not equal with the given value{}.\n{}", schemaStr.hashCode(), schemaName, schemaStr);
            }
            ThreadLocalCache.put(Constants.CacheNames.AVRO_SCHEMA_CACHE, schemaName, schemaStr);
        }
        return schemaStr;
    }

    private String generateAvroSchema(String schemaName) {

        String[] params = schemaName.split("\\.");
        if (params.length != 3) {
            return null;
        }

        String schema = AvroSchemaTemplate.genAvroSchema(params[0], params[1]);
        logger.info("Avro schema was generated:{}", Utils.replaceBlanks(schema));
        return schema;
    }

    /**
     * 读取固定的文件
     */
    private String initializeLocalSchema(String schemaName) throws Exception {
        logger.info("Initialize avro schema {}", schemaName);
        if (!schemaName.endsWith(".avsc")) {
            schemaName += ".avsc";
        }
        String schema = null;
        InputStream inputStream = null;
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream(schemaName);
            byte[] bytes = ByteStreams.toByteArray(inputStream);
            schema = new String(bytes, "utf-8");
            logger.info("Initialize avro schema {}:{}", schemaName, Utils.replaceBlanks(schema));
        } catch (Exception e) {
            logger.error("Read avro schema " + schemaName + " error", e);
            throw e;
        } finally {
            if (inputStream != null) inputStream.close();
        }
        return schema;
    }
}
