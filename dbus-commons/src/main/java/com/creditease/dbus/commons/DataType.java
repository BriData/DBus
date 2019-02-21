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

import com.creditease.dbus.commons.exception.EncodeException;
import com.creditease.dbus.enums.DbusDatasourceType;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * 定义dbus能够提供的数据类型
 * Created by Shrimp on 16/5/26.
 */
public enum DataType {
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, DATETIME, DECIMAL, BINARY ,RAW,JSONOBJECT;

    private String value;

    DataType() {
        this.value = name().toLowerCase();
    }

    @Override
    public String toString() {
        return getValue();
    }

    public String getValue() {
        return value;
    }

    public static DataType convert(String type, Integer precision, int scale) {
        switch (type) {
            case "NUMBER":
                // create table tab (num_col number;)
                // 这种情况在全量中为：precision=0,scale=-127
                // 增量中：precison=0,scale=-127
                if (precision == 0 && scale == -127) {
                    return DECIMAL;
                }
                return scale > 0 ? DECIMAL : LONG;
            case "BINARY_DOUBLE":
                return DOUBLE;
            case "BINARY_FLOAT":
                return FLOAT;
            case "FLOAT":
                return DOUBLE;
            case "DATE":
                return DATETIME;
            case "CHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "BLOB":
            case "CLOB":
            case "NCLOB":
            case "RAW":
                return STRING;
            default:
                if (type.startsWith("TIMESTAMP")) {
                    return DATETIME;
                } else {
                    return STRING;
                }
        }
    }

    public static DataType convertMysqlDataType(String type) {
        type = type.toUpperCase();
        DataType datatype = null;
        switch (type) {
            case "TINYINT":
            case "TINYINT UNSIGNED":
            case "BIT":
            case "SMALLINT":
            case "SMALLINT UNSIGNED":
            case "MEDIUMINT":
            case "INT":
            case "YEAR":
                datatype = DataType.INT;
                break;
            case "BIGINT":
            case "INT UNSIGNED":
            case "BIGINT UNSIGNED":
                datatype = DataType.LONG;
                break;
            case "FLOAT":
                datatype = DataType.FLOAT;
                break;
            case "DOUBLE":
                datatype = DataType.DOUBLE;
                break;
            case "DECIMAL":
            case "DECIMAL UNSIGNED":
                datatype = DataType.DECIMAL;
                break;
            case "DATE":
                datatype = DataType.DATE;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                datatype = DataType.DATETIME;
                break;
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "BLOB":
                datatype = DataType.BINARY;
                break;
            case "ENUM":
            case "SET":
            case "TIME":
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
                datatype = DataType.STRING;
                break;
            default:
                datatype = DataType.STRING;
                break;
        }
        return datatype;
    }


    public static DataType convertJsonLogDataType(String type) {
        type = type.toUpperCase();
        DataType datatype = null;
        switch (type) {
            case "LONG":
                datatype = DataType.LONG;
                break;
            case "DOUBLE":
                datatype = DataType.DOUBLE;
                break;
            default:
                datatype = DataType.STRING;
                break;
        }
        return datatype;
    }

    public static DataType convertMongoDataType(String type) {
        type = type.toUpperCase();
        DataType datatype = null;
        switch (type) {
            case "INTEGER":
                datatype = DataType.INT;
                break;
            case "BIGINTEGER":
            case "LONG":
                datatype = DataType.LONG;
                break;
            case "DOUBLE":
                datatype = DataType.DOUBLE;
                break;
            case "DATE":
                datatype = DataType.DATE;
                break;
            case "BIGDECIMAL":
                datatype = DataType.DECIMAL;
                break;
            case "BOOLEAN":
                datatype = DataType.BOOLEAN;
                break;
            default:
                datatype = DataType.STRING;
                break;
        }
        return datatype;
    }

    public static DataType convertDataType(String dataSourceType, String type, Integer precision, Integer scale) {
        if (DbusDatasourceType.stringEqual(dataSourceType, DbusDatasourceType.ORACLE)) {
            return convert(type, precision, scale);
        }

        if (DbusDatasourceType.stringEqual(dataSourceType, DbusDatasourceType.MYSQL)) {
            return convertMysqlDataType(type);
        }


        if (DbusDatasourceType.stringEqual(dataSourceType, DbusDatasourceType.LOG_LOGSTASH_JSON)) {
            return convertJsonLogDataType(type);
        }


        // 这里复用json的类型转换函数
        if (DbusDatasourceType.stringEqual(dataSourceType, DbusDatasourceType.ES_SQL_BATCH)) {
            return convertJsonLogDataType(type);
        }
        return null;
    }

    public static Object convertValueByDataType(DataType type, Object value) {
        if(value == null) return value;
        switch (type) {
            case DECIMAL:
            case LONG:
                // LONG类型直接输出字符串，避免java的long类型溢出
                return value.toString();
            case INT:
                return Double.valueOf(value.toString()).intValue();
            case DOUBLE:
                return Double.valueOf(value.toString());
            case FLOAT:
                return Double.valueOf(value.toString()).floatValue();
            case DATE:
            case DATETIME:
                return dateValue(value.toString());
            case BINARY:
                try {
                    //根据canal文档https://github.com/alibaba/canal/issues/18描述，针对blob、binary类型的数据，使用"ISO-8859-1"编码转换为string
                    byte[] bytes = value.toString().getBytes("ISO-8859-1");
                    return Base64.getEncoder().encodeToString(bytes);
                } catch (UnsupportedEncodingException e) {
                    throw new EncodeException("UnsupportedEncoding");
                }
            case JSONOBJECT://jsonObject也转为String
            case STRING:
                // 此项主要应对RAW类型转成String的情况，取值的时候，直接返回toString()
                // 否则RAW类型的数值，走到default的时候，不符合CharSequence.class.isInstance条件，会报错。
                return value.toString();
            default:
                if (CharSequence.class.isInstance(value)) {
                    return value.toString();
                } else {
                    throw new DataTypeException("Data type not match with String");
                }
        }
    }

    // yyyy-MM-dd:HH:mm:ss.SSSSSSSSS length = 29
    // yyyy-MM-dd:HH:mm:ss.SSSSSS length = 26
    private static String dateValue(String dateStr) {
        StringBuilder buf = new StringBuilder(dateStr);
        if (!dateStr.contains(" ")) {
            int idx = dateStr.indexOf(":");
            if (idx != -1) {
                buf.replace(idx, idx + 1, " ");
            }
        }
        if (dateStr.length() > 26) {
            buf.delete(26, dateStr.length());
        }
        return buf.toString();
    }
}
