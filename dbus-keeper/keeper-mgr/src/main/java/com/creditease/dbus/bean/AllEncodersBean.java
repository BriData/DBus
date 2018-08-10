/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.bean;

import com.creditease.dbus.domain.model.EncodePlugins;

import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-06-08
 * Time: 下午4:31
 */
public class AllEncodersBean {
    List<EncodePlugins> plugins;
    List<String> defaultEncoders;

    public List<EncodePlugins> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<EncodePlugins> plugins) {
        this.plugins = plugins;
    }

    public List<String> getDefaultEncoders() {
        return defaultEncoders;
    }

    public void setDefaultEncoders(List<String> defaultEncoders) {
        this.defaultEncoders = defaultEncoders;
    }
}
