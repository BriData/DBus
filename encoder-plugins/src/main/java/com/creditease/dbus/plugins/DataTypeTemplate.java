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


package com.creditease.dbus.plugins;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.encoders.EncoderConf;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangyf on 2018/5/3.
 */
public abstract class DataTypeTemplate implements ExtEncodeStrategy {
    protected static Logger logger = LoggerFactory.getLogger(DataTypeTemplate.class);

    @Override
    public Object encode(EncoderConf conf, Object value) {
        if (value == null) return null;

        try {
            switch (conf.getFieldType()) {
                case "decimal":
                    return encodeDecimal(conf, value);
                case "long":
                    return encodeLong(conf, value);
                case "int":
                    return encodeInt(conf, value);
                case "double":
                    return encodeDouble(conf, value);
                case "float":
                    return encodeFloat(conf, value);
                case "date":
                    return encodeDate(conf, value);
                case "datetime":
                    return encodeDatetime(conf, value);
                default:
                    return defaultEncode(conf, value);
            }
        } catch (Exception e) {
            logger.error("[message encode] Encode message error. Encode column config: {}", JSON.toJSONString(conf), e);
            return encodeOnError(conf, value);
        }
    }

    protected Object encodeOnError(EncoderConf conf, Object value) {
        ExtEncodeStrategy errorStrategy = new DefaultValueStrategy();
        return errorStrategy.encode(conf, value);
    }

    protected abstract Object encodeDecimal(EncoderConf conf, Object value);

    protected abstract Object encodeLong(EncoderConf conf, Object value);

    protected abstract Object encodeInt(EncoderConf conf, Object value);

    protected abstract Object encodeDouble(EncoderConf conf, Object value);

    protected abstract Object encodeFloat(EncoderConf conf, Object value);

    protected abstract Object encodeDate(EncoderConf conf, Object value);

    protected abstract Object encodeDatetime(EncoderConf conf, Object value);

    protected abstract Object defaultEncode(EncoderConf conf, Object value);

}
