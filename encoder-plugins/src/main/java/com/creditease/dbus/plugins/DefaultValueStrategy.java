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

/**
 * Created by zhangyf on 16/11/10.
 */
@Encoder(type = "default-value")
public class DefaultValueStrategy extends DataTypeTemplate {

    @Override
    public Object encodeDecimal(EncoderConf conf, Object value) {
        return "0";
    }

    @Override
    public Object encodeLong(EncoderConf conf, Object value) {
        return "0";
    }

    @Override
    public Object encodeInt(EncoderConf conf, Object value) {
        return 0;
    }

    @Override
    public Object encodeDouble(EncoderConf conf, Object value) {
        return 0.0;
    }

    @Override
    public Object encodeFloat(EncoderConf conf, Object value) {
        return 0.0f;
    }

    @Override
    public Object encodeDate(EncoderConf conf, Object value) {
        return "1970-01-01";
    }

    @Override
    public Object encodeDatetime(EncoderConf conf, Object value) {
        return "1970-01-01 08:00:01.000";
    }

    @Override
    public Object defaultEncode(EncoderConf conf, Object value) {
        return "*";
    }
}
