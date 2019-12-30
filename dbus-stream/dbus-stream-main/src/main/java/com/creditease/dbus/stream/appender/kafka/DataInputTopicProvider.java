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

import avro.shaded.com.google.common.collect.Sets;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.TabSchema;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * 获取输出的topic名称
 * Created by Shrimp on 16/6/15.
 */
public class DataInputTopicProvider implements TopicProvider {


    @Override
    public List<String> provideTopics(Object... args) {

        Set<String> topics = new HashSet<>();

        try {
            List<TabSchema> list = DBFacadeManager.getDbFacade().queryDataSchemas(Utils.getDatasource().getId());
            // 过滤掉没有配置的schema
            filter(list);
            for (TabSchema schema : list) {
                if (!Strings.isNullOrEmpty(schema.getSrcTopic())) {
                    topics.add(schema.getSrcTopic());
                } else {
                    topics.add(Utils.defaultOutputTopic(Utils.getDatasource().getDsName(), schema.getSchema()));
                }
            }
            return Lists.newArrayList(topics);
        } catch (Exception e) {
            throw new InitializationException();
        }
    }

    private void filter(List<TabSchema> schemaList) {
        Set<String> availableSchemas = Sets.newHashSet();

        // 获取配置指定的schema,如果没有配置则取数据库中所有schema
        String schemas = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.AVAILABLE_SCHEMAS);
        if (schemas != null) {
            String[] schemaArr = schemas.split(",");
            for (String s : schemaArr) {
                availableSchemas.add(s.trim());
            }
        }
        if (availableSchemas.isEmpty()) return;

        Iterator<TabSchema> it = schemaList.iterator();
        while (it.hasNext()) {
            TabSchema schema = it.next();
            if (!availableSchemas.contains(schema.getSchema())) {
                it.remove();
            }
        }
    }
}
