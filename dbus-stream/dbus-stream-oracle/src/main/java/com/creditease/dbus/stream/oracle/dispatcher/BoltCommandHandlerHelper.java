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


package com.creditease.dbus.stream.oracle.dispatcher;

import avro.shaded.com.google.common.collect.Maps;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.Pair;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Created by Shrimp on 16/7/1.
 */
public class BoltCommandHandlerHelper {

    private static Logger logger = LoggerFactory.getLogger(BoltCommandHandlerHelper.class);

    public static <T extends Object> PairWrapper<String, Object> convertAvroRecord(GenericRecord record, Set<T> noorderKeys) {
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        PairWrapper<String, Object> wrapper = new PairWrapper<>();

        for (Schema.Field field : fields) {
            String key = field.name();
            Object value = record.get(key);
            // 分离存储是否关心顺序的key-value
            if (noorderKeys.contains(field.name())) {
                wrapper.addProperties(key, value);
            }
        }

        GenericRecord before = getFromRecord(Constants.MessageBodyKey.BEFORE, record);
        GenericRecord after = getFromRecord(Constants.MessageBodyKey.AFTER, record);

        Map<String, Object> beforeMap = convert2map(before);
        Map<String, Object> afterMap = convert2map(after);

        // 覆盖before
        mergeMap(beforeMap, afterMap);

        for (Map.Entry<String, Object> entry : beforeMap.entrySet()) {
            if (!entry.getKey().endsWith(Constants.MessageBodyKey.IS_MISSING_SUFFIX)) {
                wrapper.addPair(new Pair<>(entry.getKey(), entry.getValue()));
            }
        }

        return wrapper;
    }

    private static <T> T getFromRecord(String key, GenericRecord record) {
        return (T) record.get(key);
    }

    private static void mergeMap(Map<String, Object> m0, Map<String, Object> m) {
        for (Map.Entry<String, Object> entry : m.entrySet()) {
            if (!entry.getKey().endsWith(Constants.MessageBodyKey.IS_MISSING_SUFFIX)) {
                if (!(Boolean) m.get(entry.getKey() + Constants.MessageBodyKey.IS_MISSING_SUFFIX)) {
                    m0.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static Map<String, Object> convert2map(GenericRecord record) {
        Map<String, Object> map = Maps.newHashMap();
        if (record != null) {
            List<Schema.Field> fields = record.getSchema().getFields();
            for (Schema.Field field : fields) {
                String key = field.name();
                map.put(key, record.get(key));
            }
        }
        return map;
    }
}
