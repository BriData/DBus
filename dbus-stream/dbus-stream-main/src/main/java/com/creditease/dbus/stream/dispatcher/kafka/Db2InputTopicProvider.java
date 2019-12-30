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


package com.creditease.dbus.stream.dispatcher.kafka;

import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.dispatcher.helper.DBHelper;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhenlinzhong on 2018/4/24.
 */
public class Db2InputTopicProvider implements TopicProvider {
    private String zkServers;
    private DataSourceInfo dsInfo;

    public Db2InputTopicProvider(String zkServers, DataSourceInfo dsInfo) {
        this.zkServers = zkServers;
        this.dsInfo = dsInfo;
    }

    @Override
    public List<String> provideTopics(Object... args) {
        Set<String> topics = new HashSet<>();

        DBHelper dbHelper = new DBHelper(zkServers);

        try {
            topics = dbHelper.loadTablesInfo(dsInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Lists.newArrayList(topics);
    }
}
