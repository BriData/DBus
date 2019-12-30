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


package com.creditease.dbus.stream.common.appender.bolt.processor;

import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.encoders.PluginLoader;
import com.creditease.dbus.stream.common.appender.bean.EncoderPluginConfig;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zhangyf on 2018/5/4.
 */
public class AppenderPluginLoader implements PluginLoader {

    @Override
    public List<EncodePlugin> loadPlugins() {
        List<EncoderPluginConfig> configs = DBFacadeManager.getDbFacade().loadEncodePlugins(0L);
        return configs.stream().map(config -> {
            EncodePlugin p = new EncodePlugin();
            p.setId(config.getId() + "");
            p.setName(config.getName());
            p.setJarPath(config.getJarPath());
            return p;
        }).collect(Collectors.toList());
    }
}
