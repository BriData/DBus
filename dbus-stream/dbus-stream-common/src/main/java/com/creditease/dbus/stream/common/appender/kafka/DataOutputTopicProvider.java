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


package com.creditease.dbus.stream.common.appender.kafka;

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.TabSchema;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;


/**
 * 获取输出的topic名称
 * Created by Shrimp on 16/6/15.
 */
public class DataOutputTopicProvider implements TopicProvider {
    @Override
    public List<String> provideTopics(Object... args) {
        String key = Utils.buildDataTableCacheKey(args[0].toString(), args[1].toString());
        DataTable t = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key);
        if (t == null) {
            return null;
        }
        List<String> topics = new ArrayList<>(1);
        if (!Strings.isNullOrEmpty(t.getOutputTopic())) {
            topics.add(t.getOutputTopic());
        } else {
            TabSchema schema = ThreadLocalCache.get(Constants.CacheNames.TAB_SCHEMA, t.getSchema());
            if (!Strings.isNullOrEmpty(schema.getTargetTopic())) {
                topics.add(schema.getTargetTopic());
            } else {
                topics.add(Utils.defaultResultTopic(Utils.getDatasource().getDsName(), t.getSchema()));
            }
        }
        return topics;
    }
}
