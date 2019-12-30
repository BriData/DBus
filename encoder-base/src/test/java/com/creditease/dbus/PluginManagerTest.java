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


package com.creditease.dbus;

import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.encoders.PluginManager;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhangyf on 2018/5/24.
 */
public class PluginManagerTest {
    @Test
    public void test() throws Exception {
        PluginManager m = new PluginManager(Encoder.class, () -> {
            List<EncodePlugin> plugins = new LinkedList<>();
            EncodePlugin plugin = new EncodePlugin();
            plugin.setId("1");
            plugin.setName("address");
            plugin.setJarPath("encoder-plugins\\target\\encoder-plugins-0.4.0.jar");
            plugins.add(plugin);

            return plugins;
        });
        ExtEncodeStrategy address = m.getPlugin("1", "address");
    }
}
