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


package com.creditease.dbus.stream.db2.dispatcher;


import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Db2GenericSchemaProvider {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ConcurrentMap<String, Schema> schemaCache;
    private ConcurrentMap<String, Integer> hashCache;
    private static Db2GenericSchemaProvider instance = new Db2GenericSchemaProvider();

    private Db2GenericSchemaProvider() {
        schemaCache = new ConcurrentHashMap<>();
        hashCache = new ConcurrentHashMap<>();
    }

    public static Db2GenericSchemaProvider getInstance() {
        return instance;
    }


    public Schema getSchema(String schemaName) throws Exception {
        Schema schema = schemaCache.get(schemaName);
        if (schema != null) {
            return schema;
        }
        String schemaStr = initializeLocalSchema(schemaName);
        schema = new Schema.Parser().parse(schemaStr);

        schemaCache.putIfAbsent(schemaName, schema);
        hashCache.putIfAbsent(schemaName, schemaStr.hashCode());

        return schema;
    }

    public int getSchemaHash(String schemaName) {
        return hashCache.get(schemaName);
    }

    /**
     * 读取固定的文件
     */
    private String initializeLocalSchema(String schemaName) throws Exception {
        logger.debug("Initialize avro schema {}", schemaName);
        if (!schemaName.endsWith(".avsc")) {
            schemaName += ".avsc";
        }
        String schema = null;
        InputStream inputStream = null;
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream(schemaName);
            byte[] bytes = ByteStreams.toByteArray(inputStream);
            schema = new String(bytes, "utf-8");
            logger.debug("Initialize avro schema {}:{}", schemaName, replaceBlanks(schema));
        } catch (IOException e) {
            logger.error("Read avro schema " + schemaName + " error", e);
            throw e;
        } finally {
            if (inputStream != null) inputStream.close();
        }
        return schema;
    }

    public String replaceBlanks(String data) {
        return data.replaceAll("\\s", "");
    }
}
