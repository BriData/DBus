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

package com.creditease.dbus.commons;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/5/20.
 */
public class DbusMessageBuilder {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private DbusMessage message;
    private String version;

    private int umsFixedFields = 0;

    public DbusMessageBuilder() {
        //缺省是1.3版本
        this(Constants.VERSION_13);
    }

    public DbusMessageBuilder(String intendVersion) {
        if (intendVersion.equals(Constants.VERSION_12)) {
            version = Constants.VERSION_12;
        } else if (intendVersion.equals(Constants.VERSION_14)) {
            version = Constants.VERSION_14;
        } else {
            version = Constants.VERSION_13;
        }
    }

    public DbusMessageBuilder build(DbusMessage.ProtocolType type, String schemaNs, int batchNo) {
        if (version.equals(Constants.VERSION_12)) {
            message = new DbusMessage12(version, type, schemaNs);
        } else if (version.equals(Constants.VERSION_14)) {
            message = new DbusMessage14(version, type, schemaNs, batchNo);
        } else {
            message = new DbusMessage13(version, type, schemaNs, batchNo);
        }

        // 添加schema中的默认值
        switch (type) {
            case DATA_INCREMENT_DATA:
            case DATA_INITIAL_DATA:
                // 添加schema中的默认值
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                message.getSchema().addField(DbusMessage.Field._UMS_OP_, DataType.STRING, false);
                umsFixedFields = 3;
                switch (version) {
                    case Constants.VERSION_12:
                        break;
                    default:
                        // 1.3 or later
                        message.getSchema().addField(DbusMessage.Field._UMS_UID_, DataType.STRING, false);
                        umsFixedFields++;
                        break;
                }
                break;
            case DATA_INCREMENT_TERMINATION:
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                umsFixedFields = 1;
                break;
            case DATA_INCREMENT_HEARTBEAT:
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                umsFixedFields = 2;
                break;
            default:
                break;
        }

        return this;
    }

    /**
     * DbusMessager14 unsetFeild的处理
     *
     * @param type
     * @param schemaNs
     * @param batchNo
     * @param unsetFiled
     * @return
     */
    public DbusMessageBuilder build(DbusMessage.ProtocolType type, String schemaNs, int batchNo, List<DbusMessage.Field> unsetFiled) {
        if (!version.equals(Constants.VERSION_14))
            return null;

        message = new DbusMessage14(version, type, schemaNs, batchNo, unsetFiled);
        // 添加schema中的默认值
        switch (type) {
            case DATA_INCREMENT_DATA:
            case DATA_INITIAL_DATA:
                // 添加schema中的默认值
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                message.getSchema().addField(DbusMessage.Field._UMS_OP_, DataType.STRING, false);
                message.getSchema().addField(DbusMessage.Field._UMS_UID_, DataType.STRING, false);
                umsFixedFields = 4;
                break;
            case DATA_INCREMENT_TERMINATION:
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                umsFixedFields = 1;
                break;
            case DATA_INCREMENT_HEARTBEAT:
                message.getSchema().addField(DbusMessage.Field._UMS_ID_, DataType.LONG, false);
                message.getSchema().addField(DbusMessage.Field._UMS_TS_, DataType.DATETIME, false);
                umsFixedFields = 2;
                break;
            default:
                break;
        }
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver) {
        return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, "0", "0");
    }

    public String buildNameSpace(String datasourceNs, String dbSchema, String table, int ver, String partitionTable) {
        return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, "0", partitionTable);
    }

    public DbusMessageBuilder appendSchema(String name, DataType type, boolean nullable) {
        validateState();
        message.getSchema().addField(name, type, nullable);
        return this;
    }

    public DbusMessageBuilder appendSchema(String name, DataType type, boolean nullable, boolean encoded) {
        validateState();
        message.getSchema().addField(name, type, nullable, encoded);
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

    public int getUmsFixedFields() {
        return umsFixedFields;
    }

    /**
     * 校验输入数据和schema是否匹配
     *
     * @param tuple
     */
    private void validateAndConvert(Object[] tuple) {
        List<DbusMessage.Field> fields = message.getSchema().getFields();
        if (tuple.length != fields.size()) {
            throw new IllegalArgumentException(String.format("Data fields length != schema field length!!  data_length=%d, schema_length=%d",
                        tuple.length, fields.size()));
        }
        DbusMessage.Field field;
        for (int i = 0; i < fields.size(); i++) {
            field = fields.get(i);
            Object value = tuple[i];
            // 增加默认值的处理后不再进行非空校验
            // 判断是否为空
//            if (value == null) {
//                if (field.isNullable()) continue;
//                throw new NullPointerException("Field " + field.getName() + " can not bet set a null value");
//            }

            try {
                tuple[i] = DataType.convertValueByDataType(field.dataType(), value);
            } catch (DataTypeException e) {
                logger.error("Data type '{}' of filed '{}' not match with String", field.dataType(), field.getName());
                throw e;
            } catch (NullPointerException e) {
                logger.error("NullPointerException Data type '{}' of filed '{}'", field.dataType(), field.getName());
                throw e;
            }
        }
    }

    private void validateState() {
        if (message == null) {
            throw new IllegalStateException("DbusMessage has bean not built, please call DbusMessageBuilder.build() method ahead");
        }
    }

    //testing
    public static void main(String[] args) {
        List<Object> rowDataValues = new ArrayList<>();

        DbusMessageBuilder builder = new DbusMessageBuilder();
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, "dstype.ds1.schema1.table1.5.0.0", 3);
        builder.appendSchema("col1", DataType.INT, false);
        rowDataValues.clear();
        rowDataValues.add(50000012354L);
        rowDataValues.add("2018-01-01 10:13:24.2134");
        rowDataValues.add("i");
        rowDataValues.add("12345"); //uid
        rowDataValues.add(13);
        builder.appendPayload(rowDataValues.toArray());
        System.out.println(builder.message.toString());

        builder = new DbusMessageBuilder(Constants.VERSION_12);
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, "dstype.ds1.schema1.table1.5.0.0", 3);
        builder.appendSchema("col1", DataType.INT, false);
        rowDataValues.clear();
        rowDataValues.add(50000012354L);
        rowDataValues.add("2018-01-01 10:13:24.2134");
        rowDataValues.add("i");
        rowDataValues.add(13);
        builder.appendPayload(rowDataValues.toArray());
        System.out.println(builder.message.toString());

        builder = new DbusMessageBuilder(Constants.VERSION_13);
        builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, "dstype.ds1.schema1.table1.5.0.0", 3);
        builder.appendSchema("col1", DataType.INT, false);
        rowDataValues.clear();
        rowDataValues.add(50000012354L);
        rowDataValues.add("2018-01-01 10:13:24.2134");
        rowDataValues.add("i");
        rowDataValues.add("12345");
        rowDataValues.add(13);
        builder.appendPayload(rowDataValues.toArray());
        System.out.println(builder.message.toString());
    }
}
