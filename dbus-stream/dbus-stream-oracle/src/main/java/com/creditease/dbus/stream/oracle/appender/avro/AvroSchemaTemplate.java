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

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 生成Avro Schema的模板
 * Created by Shrimp on 16/6/24.
 */
public class AvroSchemaTemplate {
    private static final String CELL;
    private static final String TEMPLATE;

    static {
        try {
            ClassLoader classLoader = AvroSchemaTemplate.class.getClassLoader();
            byte[] bytes = ByteStreams.toByteArray(classLoader.getResourceAsStream("schema_template_fieldunit.json"));
            CELL = new String(bytes, "utf-8").trim();
            bytes = ByteStreams.toByteArray(classLoader.getResourceAsStream("schema_template.json"));
            TEMPLATE = new String(bytes, "utf-8").trim();
        } catch (IOException e) {
            throw new InitializationException(e);
        }
    }

    /**
     * 根据database schema名以及table 生成avro schema
     *
     * @param dbSchema
     * @param table
     * @return
     */
    public static String genAvroSchema(String dbSchema, String table) {
        MetaVersion ver = ThreadLocalCache.get(Constants.CacheNames.META_VERSION_CACHE, genKey(dbSchema, table));
        StringBuilder buf = new StringBuilder();
        List<MetaWrapper.MetaCell> columns = ver.getMeta().getColumns();

        final boolean includeVirtualColumns = includeVirtualFields();
        // 新生成一个列表，增加一个filter来处理配置忽略虚拟列的问题
        List<Column> columnList = columns.stream()
                .filter(column -> includeVirtualColumns || (!column.isVirtual() && !column.isHidden()))
                .map(column -> new Column(column.getColumnName(), column.getInternalColumnId())).collect(Collectors.toList());

        // 重新按照meta中的internal_column_id升序排列
        columnList.sort((c1, c2) -> c1.columnId - c2.columnId);
        for (Column column : columnList) {
            buf.append(String.format(CELL, column.name, column.name)).append(", ");
        }

        buf.delete(buf.length() - 2, buf.length() - 1);
        return String.format(TEMPLATE, table, dbSchema, buf.toString());
    }

    /**
     * 新版的oracle forbigdata解决了函数索引的问题，这里判断是否配置了需要忽略虚拟列和隐藏列
     *
     * @return
     */
    private static boolean includeVirtualFields() {
        Integer configValue = PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, Constants.ConfigureKey.IGNORE_VIRTUAL_FIELDS);
        return configValue == null || configValue == 0;
    }

    /**
     * 生成datasource.schema.table格式的key值
     */
    public static String genKey(String schema, String tableName) {
        return Utils.buildDataTableCacheKey(schema, tableName);
    }

    private static final class Column {
        private String name;
        private int columnId;

        public Column(String name, int columnId) {
            this.name = name;
            this.columnId = columnId;
        }
    }
}
