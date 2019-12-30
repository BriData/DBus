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


package com.creditease.dbus.router.encode;

import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.encoders.PluginLoader;
import com.creditease.dbus.router.facade.DBFacade;

import java.util.List;

/**
 * Created by mal on 2018/5/28.
 */
public class DBusRouterPluginLoader implements PluginLoader {

    private DBFacade dbHelper;

    private String topologyName;

    public DBusRouterPluginLoader(DBFacade dbHelper, String topologyName) {
        this.dbHelper = dbHelper;
        this.topologyName = topologyName;
    }

    @Override
    public List<EncodePlugin> loadPlugins() {
        try {
            return dbHelper.loadEncodePlugins(topologyName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
