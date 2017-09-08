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

package com.creditease.dbus.commons;

import com.creditease.dbus.commons.exception.EncodeException;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;

/**
 * Created by Shrimp on 16/5/20.
 */
public class DbusMessageBuilder {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final String VERSION = "1.3";
    private DbusMessage message;
    public DbusMessageBuilder() {
    }

    public DbusMessageBuilder build(DbusMessage.ProtocolType type, String schemaNs, int batchNo) {
        message = new DbusMessage(type, schemaNs, batchNo);
        message.getProtocol().setVersion(VERSION);
        // 添加schema中的默认值
        switch (type) {
            case DATA_INCREMENT_DATA:
            case DATA_INITIAL_DATA :
                // 添加schema中的默认值
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                message.getSchema().addField(DbusMessage.Field._UMS_OP_, DataType.STRING, false);
                message.getSchema().addField(DbusMessage.Field._UMS_UID_, DataType.STRING, false);
                break;
            case DATA_INCREMENT_TERMINATION:
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                break;
            case DATA_INCREMENT_HEARTBEAT:
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                break;
            default:
                break;
        }

        return this;
    }

    public String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver) {
        return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, "0", "0");
    }
    
    public String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver, String partitionTable){
    	return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, "0", partitionTable);
    }

    public DbusMessageBuilder  appendSchema(String name, DataType type, boolean nullable) {
        validateState();
        message.getSchema().addField(name, type, nullable);
        return this;
    }

    public DbusMessageBuilder appendPayload(Object[] tuple) {
        validateState();
        validateAndConvert(tuple);
        message.addTuple(tuple);

        return this;
    }

    public DbusMessage getMessage() {
        validateState(); // 验证
        return message;
    }

    /**
     * 校验输入数据和schema是否匹配
     *
     * @param tuple
     */
    private void validateAndConvert(Object[] tuple) {
        List<DbusMessage.Field> fields = message.getSchema().getFields();
        if (tuple.length != fields.size()) {
            throw new IllegalArgumentException("Data length can't match with the field size of the message schema");
        }
        DbusMessage.Field field = null;
        Class<?> clazz = null;
        for (int i = 0; i < fields.size(); i++) {
            field = fields.get(i);
            Object value = tuple[i];
            // 判断是否为空
            if (value == null) {
                if (field.isNullable()) {
                    continue;
                }
                throw new NullPointerException("Field " + field.getName() + " can not bet set a null value");
            }

            switch (field.dataType()) {
                case DECIMAL:
                    tuple[i] =value.toString();
                    break;
                case LONG:
                    // LONG类型直接输出字符串，避免java的long类型溢出
                    tuple[i] = value.toString();
                    break;
                case INT:
                    tuple[i] = Double.valueOf(value.toString()).intValue();
                    break;
                case DOUBLE:
                    tuple[i] = Double.valueOf(value.toString());
                    break;
                case FLOAT:
                    tuple[i] = Double.valueOf(value.toString()).floatValue();
                    break;
                case DATE:
                    String val = value.toString();
                    tuple[i] = dateValue(val);
                    break;
                case DATETIME:
                    val = value.toString();
                    tuple[i] = dateValue(val);
                    break;
                case BINARY:
                    try {
                        //根据canal文档https://github.com/alibaba/canal/issues/18描述，针对blob、binary类型的数据，使用"ISO-8859-1"编码转换为string
                        byte[] bytes = value.toString().getBytes("ISO-8859-1");
                        tuple[i] = Base64.getEncoder().encodeToString(bytes);
                    } catch (UnsupportedEncodingException e) {
                        throw new EncodeException("UnsupportedEncoding");
                    }
                    break;
                default:
                    if (CharSequence.class.isInstance(value)) {
                        tuple[i] = value.toString();
                        break;
                    }
                    logger.error("Data type '{}' of filed '{}' not match with String",field.dataType(),field.getName());
                    throw new DataTypeException("Data type not match with String");
            }
        }
    }

    // yyyy-MM-dd:HH:mm:ss.SSSSSSSSS length = 29
    // yyyy-MM-dd:HH:mm:ss.SSSSSS length = 26
    private static String dateValue(String dateStr) {
        StringBuilder buf = new StringBuilder(dateStr);
        if(!dateStr.contains(" ")) {
            int idx = dateStr.indexOf(":");
            if(idx!=-1){
                buf.replace(idx, idx + 1, " ");
            }
        }
        if (dateStr.length() > 26) {
            buf.delete(26, dateStr.length());
        }
        return buf.toString();
    }

    private void validateState() {
        if (message == null) {
            throw new IllegalStateException("DbusMessage has bean not built, please call DbusMessageBuilder.build() method ahead");
        }
    }

    public static void main(String[] args) {
        System.out.println(dateValue("2012-12-12:12:12:12"));
    }
}
