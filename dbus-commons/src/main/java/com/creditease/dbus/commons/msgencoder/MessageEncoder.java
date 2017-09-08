/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.commons.msgencoder;

import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.enums.MessageEncodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 16/11/9.
 */
public class MessageEncoder {
    private static Logger logger = LoggerFactory.getLogger(MessageEncoder.class);
    private List<String> saltColumns;
    private Map<String, EncodeStrategy> encoders;

    public MessageEncoder() {
        saltColumns = new ArrayList<>();
        encoders = new HashMap<>();
    }

    public void encode(DbusMessage message, EncodeColumnProvider provider) {
        this.encode(message, provider.getColumns());
    }

    public void encode(DbusMessage message, List<EncodeColumn> columns) {
        // 抽取加盐使用到的列，并且生成encoder对象
        this.buildEncoders(columns);
        for (int i = 0; i < message.getPayload().size(); i++) {
            // 获取每一行的salt值
            Map<String, Object> saltValues = extractSaltValue(message, i);
            for (EncodeColumn column : columns) {
                EncodeStrategy encoder = getEncoder(column.getFieldName());
                String encoderName = encoder.getClass().getSimpleName();
                Object before = message.messageValue(column.getFieldName(), i);
                DbusMessage.Field field = message.getSchema().field(column.getFieldName());

                Object salt = saltValues.get(column.getEncodeParam());
                encoder.set(salt); // 扩展点
                Object after = encoder.encode(field, before, column);
                if (column.isTruncate() && DataType.STRING == field.dataType() && after != null) {
                    String strAfter = after.toString();
                    if (strAfter.length() > column.getLength()) {
                        after = strAfter.substring(0, column.getLength());
                    }
                }
                message.setMessageValue(column.getFieldName(), after, i);
                field.setEncoded(true); // 标记字段被脱敏过

                if (after != null) {
                    logger.debug("[message encode] Message of {}.{}[{}] was encoded by {}, before:{}, salt:{}, after:{}[{}]",
                            message.getSchema().getNamespace(), field.getName(), field.getType(), encoderName, before, salt, after, after.getClass());
                } else {
                    logger.debug("[message encode] Message of {}.{}[{}] was encoded by {}, before:{}, after:{}",
                            message.getSchema().getNamespace(), field.getName(), field.getType(), encoderName, before, salt, after);
                }
            }
        }
    }


    private void buildEncoders(List<EncodeColumn> columns) {
        for (EncodeColumn c : columns) {
            String type = c.getEncodeType();
            MessageEncodeType t = MessageEncodeType.parse(type);

            if(t.equals(MessageEncodeType.YISOU_DATA_CLEAN)){ t = null;}
            if (t == null) {
                Map<String, Class<ExtEncodeStrategy>> map = ExternalEncoders.get();
                Class<ExtEncodeStrategy> extEncoderClass = map.get(type);
                if (extEncoderClass != null) {
                    try {
                        ExtEncodeStrategy extEncoder = extEncoderClass.newInstance();
                        addEncoder(c.getFieldName(), new ExtEncoderAdapter(extEncoder));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    addEncoder(c.getFieldName(), new DefaultValueStrategy());
                }
                continue;
            }
            switch (t) {
                case REPLACE:
                    addEncoder(c.getFieldName(), new ReplacementStrategy());
                    break;
                case HASH_MD5:
                    addEncoder(c.getFieldName(), new Md5HashStrategy());
                    break;
                case HASH_MURMUR:
                    addEncoder(c.getFieldName(), new Murmur3HashStrategy());
                    break;
                case HASH_MD5_FIELD_SALT:
                    saltColumns.add(c.getEncodeParam());
                    addEncoder(c.getFieldName(), new Md5FieldSaltStrategy());
                    break;
                case HASH_MD5_FIXED_SALT:
                    addEncoder(c.getFieldName(), new Md5FixedSaltStrategy());
                    break;
//                case ADDRESS_NORMALIZE:
//                    addEncoder(c.getFieldName(), new AddressNormalizerEncoder());
//                    break;
                case REGEX_NORMALIZE:
                    addEncoder(c.getFieldName(), new RegexNormalizerEncoder());
                    break;
                default:
                    addEncoder(c.getFieldName(), new DefaultValueStrategy());
            }
        }
    }

    private EncodeStrategy getEncoder(String key) {
        return this.encoders.get(key);
    }

    private void addEncoder(String key, EncodeStrategy encoder) {
        this.encoders.put(key, encoder);
    }

    private Map<String, Object> extractSaltValue(DbusMessage message, int idx) {
        Map<String, Object> map = new HashMap<>();
        for (String c : saltColumns) {
            map.put(c, message.messageValue(c, idx));
        }
        return map;
    }
}
