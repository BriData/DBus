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

import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.EncoderConf;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 非线程安全的实现
 * Created by zhangyf on 16/11/10.
 */
@Encoder(type = "replace")
public class ReplacementStrategy extends DataTypeTemplate {

    private DateFormat dtf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private DateFormat dtf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public Object encodeDecimal(EncoderConf conf, Object value) {
        return new BigDecimal(conf.getEncodeParam()).toString();
    }

    @Override
    public Object encodeLong(EncoderConf conf, Object value) {
        return Long.valueOf(conf.getEncodeParam()).toString();
    }

    @Override
    public Object encodeInt(EncoderConf conf, Object value) {
        return Integer.parseInt(conf.getEncodeParam());
    }

    @Override
    public Object encodeDouble(EncoderConf conf, Object value) {
        return Double.parseDouble(conf.getEncodeParam());
    }

    @Override
    public Object encodeFloat(EncoderConf conf, Object value) {
        return Float.parseFloat(conf.getEncodeParam());
    }

    @Override
    public Object encodeDate(EncoderConf conf, Object value) {
        try {
            df.parse(conf.getEncodeParam());
            return conf.getEncodeParam();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object encodeDatetime(EncoderConf conf, Object value) {
        parse(conf.getEncodeParam()); // 验证格式
        return conf.getEncodeParam();
    }

    @Override
    public Object defaultEncode(EncoderConf conf, Object value) {
        return conf.getEncodeParam();
    }

    private void parse(String datetimeStr) {
        int length = datetimeStr.length();
        try {
            if (length == 19) {
                dtf1.parse(datetimeStr);
            } else if (length == 23) {
                dtf2.parse(datetimeStr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
