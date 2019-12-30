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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.DbusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by zhangyf on 16/11/10.
 */
@Deprecated
public class ReplacementStrategy implements EncodeStrategy {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private EncodeStrategy errorStrategy;
    private DateFormat dtf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private DateFormat dtf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    public ReplacementStrategy() {
        this.errorStrategy = new DefaultValueStrategy();
    }

    @Override
    public Object encode(DbusMessage.Field field, Object value, EncodeColumn col) {
        if (value == null) return null;
        if (col.getEncodeParam() == null || col.getEncodeParam().trim().equals("")) {
            return errorStrategy.encode(field, value, col);
        }
        try {
            switch (field.dataType()) {
                case DECIMAL:
                    return new BigDecimal(col.getEncodeParam()).toString();
                case LONG:
                    return Long.valueOf(col.getEncodeParam()).toString();
                case INT:
                    return Integer.parseInt(col.getEncodeParam());
                case DOUBLE:
                    return Double.parseDouble(col.getEncodeParam());
                case FLOAT:
                    return Float.parseFloat(col.getEncodeParam());
                case DATE:
                    df.parse(col.getEncodeParam());
                    return col.getEncodeParam();
                case DATETIME:
                    parse(col.getEncodeParam());
                    return col.getEncodeParam();
                default:
                    return col.getEncodeParam();
            }
        } catch (Exception e) {
            logger.error("[message encode] Encode message error. Encode column config: {}", JSON.toJSONString(col));
            return errorStrategy.encode(field, value, col);
        }
    }

    private void parse(String datetimeStr) throws Exception {
        int length = datetimeStr.length();
        if (length == 19) {
            dtf1.parse(datetimeStr);
        } else if (length == 23) {
            dtf2.parse(datetimeStr);
        }
    }
}
