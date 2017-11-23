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

import com.creditease.dbus.enums.DbusDatasourceType;

/**
 * 定义dbus能够提供的数据类型
 * Created by Shrimp on 16/5/26.
 */
public enum DataType {
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, DATETIME, DECIMAL, BINARY;

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
                return STRING;
            case "VARCHAR2":
                return STRING;
            case "NCHAR":
                return STRING;
            case "NVARCHAR2":
                return STRING;
            case "BLOB":
                return STRING;
            case "CLOB":
                return STRING;
            case "NCLOB":
                return STRING;
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

    public static DataType convertDataType(String dataSourceType, String type, Integer precision, Integer scale) {
        if (dataSourceType.equalsIgnoreCase(DbusDatasourceType.ORACLE.name())) {
            return convert(type, precision, scale);
        }

        if (dataSourceType.equalsIgnoreCase(DbusDatasourceType.MYSQL.name())) {
            return convertMysqlDataType(type);
        }

        if (dataSourceType.equalsIgnoreCase(DbusDatasourceType.JSONLOG.name())) {
            return convertJsonLogDataType(type);
        }
        return null;
    }
}
