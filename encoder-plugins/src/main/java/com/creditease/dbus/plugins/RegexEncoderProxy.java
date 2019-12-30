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

import com.creditease.dbus.encoders.EncoderConf;
import com.creditease.dbus.encoders.ExtEncodeStrategy;

/**
 * Created by zhangyf on 2018/5/10.
 */
public abstract class RegexEncoderProxy implements ExtEncodeStrategy, RegexStrategy.ParamBuilder {
    private ExtEncodeStrategy strategy;
    protected String value;

    public RegexEncoderProxy() {
        strategy = new RegexStrategy();
    }

    @Override
    public Object encode(EncoderConf conf, Object value) {
        if (value == null || value.toString().trim().length() == 0) return value;
        hook(conf, value);
        String param = buildParameter(buildConfig());
        conf.setEncodeParam(param);
        return strategy.encode(conf, value);
    }

    protected void hook(EncoderConf conf, Object value) {
        this.value = value.toString().trim();
    }

    protected abstract RegexStrategy.RegexEncoderConfig buildConfig();
}
