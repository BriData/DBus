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

import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.PluginLoader;
import com.creditease.dbus.encoders.PluginManager;

/**
 * Created by zhangyf on 2018/5/4.
 */
public final class PluginManagerProvider {
    private static PluginManager manager;
    private static volatile boolean initialized = false;

    public static synchronized void initialize(PluginLoader loader) {
        if (initialized) return;
        manager = createManager(loader);
        initialized = true;
    }

    public static void reloadManager() {
        try {
            manager.reload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static PluginManager getManager() {
        if (!initialized) {
            throw new IllegalStateException("PluginManagerProvider has not been initialized.");
        }
        return manager;
    }

    private static PluginManager createManager(PluginLoader loader) {
        try {
            return new PluginManager(Encoder.class, loader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
