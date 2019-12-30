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


package com.creditease.dbus.encoders;

/**
 * Created by zhangyf on 2018/5/10.
 */
public abstract class StrategyDecorator implements ExtEncodeStrategy {
    protected ExtEncodeStrategy strategy;

    public StrategyDecorator(ExtEncodeStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public Object encode(EncoderConf conf, Object value) {
        return strategy.encode(conf, value);
    }
}
