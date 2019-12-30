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


package com.creditease.dbus.msgencoder;

import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.encoders.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 2018/5/4.
 */
public class PluggableMessageEncoder implements UmsEncoder {
    private static Logger logger = LoggerFactory.getLogger(PluggableMessageEncoder.class);
    private PluginManager manager;
    private EncodeCallback callbackHandler;
    private Map<String, EncodeStrategy> encoders;

    public PluggableMessageEncoder(PluginManager manager, EncodeCallback callback) {
        encoders = new HashMap<>();
        this.manager = manager;
        this.callbackHandler = callback;
    }

    @Override
    public void encode(DbusMessage message, EncodeColumnProvider provider) {
        this.encode(message, provider.getColumns());
    }

    @Override
    public void encode(DbusMessage message, List<? extends EncodeColumn> columns) {
        //抽取加盐使用到的列，并且生成encoder对象
        this.buildEncoders(columns);
        int messageSize = message.getPayload().size();
        List<Map<String, Object>> results = new ArrayList<>(messageSize);
        for (int i = 0; i < messageSize; i++) {
            Map<String, Object> row = extractRowValue(message, i);
            for (EncodeColumn column : columns) {
                try {
                    DbusMessage.Field field = message.getSchema().field(column.getFieldName());

                    // 列不存在的情况直接跳过
                    if (field == null) continue;

                    // 避免重复脱敏
                    if (field.isEncoded()) continue;

                    Object before = message.messageValue(column.getFieldName(), i);
                    EncodeStrategy encoder = getEncoder(column.getFieldName());
                    String encoderName = encoder.getClass().getSimpleName();

                    encoder.set(row);
                    Object after = encoder.encode(field, before, column);
                    if (column.isTruncate() && DataType.STRING == field.dataType() && after != null) {
                        String strAfter = after.toString();
                        if (strAfter.length() > column.getLength()) {
                            after = strAfter.substring(0, column.getLength());
                        }
                    }

                    // 这里不直接修改message，避免出现encode错误无法获取message原始内容
                    addToResults(results, column.getFieldName(), after, i);
                    //message.setMessageValue(column.getFieldName(), after, i);
                    //field.setEncoded(true); // 标记字段被脱敏过

                    if (logger.isDebugEnabled()) {
                        if (after != null) {
                            logger.debug("[message encode] Message of {}.{}[{}] was encoded by {}, before:{}, after:{}[{}]",
                                    message.getSchema().getNamespace(), field.getName(), field.getType(), encoderName, before, after, after.getClass());
                        } else {
                            logger.debug("[message encode] Message of {}.{}[{}] was encoded by {}, before:{}, after:{}",
                                    message.getSchema().getNamespace(), field.getName(), field.getType(), encoderName, before, after);
                        }
                    }
                } catch (Exception e) {
                    logger.error("encoding error. {tableId:\"{}\", filed:\"{}\", pluginId:\"{}\", encodeType:\"{}\", param:\"{}\"}",
                            column.getTableId(), column.getFieldName(), column.getPluginId(), column.getEncodeType(), column.getEncodeParam(), e);
                    callbackHandler.callback(e, column, message);
                }
            }
        }

        // encode完成后同意修改message
        DbusMessage.Field field;
        for (int i = 0; i < results.size(); i++) {
            for (Map.Entry<String, Object> r : results.get(i).entrySet()) {
                message.setMessageValue(r.getKey(), r.getValue(), i);
                field = message.getSchema().field(r.getKey());
                if (!field.isEncoded()) field.setEncoded(true);
            }
        }
    }

    private void buildEncoders(List<? extends EncodeColumn> columns) {
        for (EncodeColumn c : columns) {
            ExtEncodeStrategy extEncoder = manager.getPlugin(c.getPluginId() + "", c.getEncodeType());
            if (extEncoder == null) {
                throw new IllegalArgumentException("encoder not found. {id: " + c.getId() + " pluginId: " + c.getPluginId() + ", encoderType:" + c.getEncodeType() + "}");
            }
            EncodeStrategy es = new ExtEncoderAdapter(extEncoder);
            addEncoder(c.getFieldName(), es);
        }
    }

    private EncodeStrategy getEncoder(String key) {
        return this.encoders.get(key);
    }

    private void addEncoder(String key, EncodeStrategy encoder) {
        this.encoders.put(key, encoder);
    }

    private Map<String, Object> extractRowValue(DbusMessage message, int idx) {
        Map<String, Object> map = new HashMap<>();
        for (DbusMessage.Field field : message.getSchema().getFields()) {
            map.put(field.getName(), message.messageValue(field.getName(), idx));
        }
        return map;
    }

    /**
     * 将message payload每一条记录中脱敏过的字段缓存在list map中
     */
    private void addToResults(List<Map<String, Object>> results, String key, Object val, int idx) {
        Map<String, Object> result;
        if (results.size() <= idx) {
            result = new HashMap<>();
            result.put(key, val);
            results.add(idx, result);
        } else {
            result = results.get(idx);
        }
        result.put(key, val);
    }
}
