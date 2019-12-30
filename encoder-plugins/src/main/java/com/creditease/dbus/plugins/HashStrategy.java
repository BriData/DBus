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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.encoders.EncoderConf;

import java.nio.charset.Charset;

/**
 * Created by zhangyf on 16/11/10.
 */
public abstract class HashStrategy extends DataTypeTemplate implements SaltProvider {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    protected abstract HashCode hash(Object str, EncoderConf conf);

    @Override
    public Object encodeDecimal(EncoderConf conf, Object value) {
        return String.format("%.1f", hash(value, conf).asLong() / 3.0);
    }

    @Override
    public Object encodeLong(EncoderConf conf, Object value) {
        return hash(value, conf).asLong();
    }

    @Override
    public Object encodeInt(EncoderConf conf, Object value) {
        return hash(value, conf).asInt();
    }

    @Override
    public Object encodeDouble(EncoderConf conf, Object value) {
        return hash(value, conf).asLong() / 2.0d;
    }

    @Override
    public Object encodeFloat(EncoderConf conf, Object value) {
        return hash(value, conf).asInt() / 2.0f;
    }

    @Override
    public Object encodeDate(EncoderConf conf, Object value) {
        return new DefaultValueStrategy().encode(conf, value);
    }

    @Override
    public Object encodeDatetime(EncoderConf conf, Object value) {
        return new DefaultValueStrategy().encode(conf, value);
    }

    @Override
    public Object defaultEncode(EncoderConf conf, Object value) {
        return hash(value, conf).toString();
    }

    @Override
    public String getSalt(EncoderConf conf) {
        String param = conf.getEncodeParam();
        if (param != null && param.trim().length() > 0) {
            Md5HashStrategy.Salt salt = parseParam(param);
            if (Md5HashStrategy.Salt.TYPE_FIXED.equals(salt.getType())) {
                return salt.getSalt();
            } else if (Md5HashStrategy.Salt.TYPE_FIELD.equals(salt.getType())) {
                Object obj = conf.getRaw().get(salt.getSalt());
                if (obj == null) return "";
                return obj.toString();
            } else {
                throw new IllegalArgumentException("unknown salt type:" + salt.getType());
            }
        } else {
            return "";
        }
    }

    private Salt parseParam(String param) {
        try {
            return JSONObject.parseObject(param, Md5HashStrategy.Salt.class);
        } catch (Exception e) {
            return new Salt(Md5HashStrategy.Salt.TYPE_FIXED, param);
        }
    }

    public static class Salt {
        public static final String TYPE_FIXED = "fixed";
        public static final String TYPE_FIELD = "field";
        private String type;
        private String salt;

        public Salt() {
        }

        public Salt(String type, String salt) {
            this.type = type;
            this.salt = salt;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getSalt() {
            return salt;
        }

        public void setSalt(String salt) {
            this.salt = salt;
        }
    }
}
