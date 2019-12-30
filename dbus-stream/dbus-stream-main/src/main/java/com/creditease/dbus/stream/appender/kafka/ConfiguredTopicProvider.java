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


package com.creditease.dbus.stream.appender.kafka;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Shrimp on 16/6/7.
 */
public class ConfiguredTopicProvider implements TopicProvider {
    private String configName;

    public ConfiguredTopicProvider(String configName) {
        this.configName = configName;
    }

    @Override
    public List<String> provideTopics(Object... args) {
        List<String> list = new LinkedList<>();
        for (Object propertiesKey : args) {
            split2Set(list, PropertiesHolder.getProperties(configName, propertiesKey.toString()));
        }
        return list;
    }

    protected void split2Set(List<String> list, String s) {
        if (s == null) return;
        String[] arr = s.split(",");
        for (String t : arr) {
            list.add(t.trim());
        }
    }
}
