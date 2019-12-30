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


package com.creditease.dbus.stream.common.appender.cache;

import com.creditease.dbus.stream.common.Constants.CacheNames;
import com.creditease.dbus.stream.common.appender.bean.AvroSchema;
import com.creditease.dbus.stream.common.appender.utils.DBFacade;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/8/17.
 */
public class DbusCacheLoader implements LocalCacheLoader {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Object load(String type, String key) {
        Object result = null;
        try {
            DBFacade db = DBFacadeManager.getDbFacade();
            long dsId = Utils.getDatasource().getId();
            SchemaTable st;
            switch (type) {
                case CacheNames.TAB_SCHEMA:
                    logger.info("Query table schema from database with parameter:{}", key);
                    result = db.queryDataSchema(dsId, key);
                    break;
                case CacheNames.DATA_TABLES:
                    logger.info("Query tables from database with parameter:{}", key);
                    st = SchemaTable.parse(key);
                    result = db.queryDataTable(dsId, st.schema, st.table);
                    break;
                case CacheNames.META_VERSION_CACHE:
                    logger.info("Query meta version from database with parameter:{}", key);
                    st = SchemaTable.parse(key);
                    result = db.queryMetaVersion(dsId, st.schema, st.table);
                    break;
                case CacheNames.AVRO_SCHEMA_CACHE:
                    logger.info("Query avro schema from database with parameter:{}", key);
                    AvroSchema schema = db.queryAvroSchema(key, dsId);
                    if (schema != null)
                        result = schema.getSchema();
                    break;
                case CacheNames.TAB_ENCODE_FIELDS:
                    logger.info("Query encode table columns from database with parameter:{}", key);
                    result = db.getEncodeColumns(Long.parseLong(key));
                    break;
                case CacheNames.TAB_CENCODE_PLUGINS:
                    logger.info("Query encode plugins from database with parameter:{}", key);
                    result = db.loadEncodePlugins(Long.parseLong(key));
                    break;
                default:
                    logger.warn("Unsupported type[{}] of CacheLoader", type);
            }
        } catch (Exception e) {
            logger.error("Fail to get Object from database with parameter:{type:'{}',key:'{}'", type, key, e);
        }
        return result;
    }

    private static class SchemaTable {
        String schema;
        String table;

        public static SchemaTable parse(String schemaTable) {
            SchemaTable st = new SchemaTable();
            int idx = schemaTable.indexOf(".");
            st.schema = schemaTable.substring(0, idx);
            st.table = schemaTable.substring(idx + 1, schemaTable.length());
            return st;
        }
    }
}
