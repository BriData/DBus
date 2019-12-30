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


package com.creditease.dbus.stream.appender.utils;

import com.creditease.dbus.stream.common.Constants;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 1.判断emit数据的最后一个字段是否为0, 如果为0则发给所有的task
 * 2.不满足条件1的情况,按照第一个字段hash code与task数取余选择task
 * Created by Shrimp on 16/8/2.
 */
public class DbusGrouping implements CustomStreamGrouping {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private List<List<Integer>> choices;
    private List<Integer> allTasks;

    public DbusGrouping() {
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        choices = new ArrayList<>(targetTasks.size());
        allTasks = new ArrayList<>(targetTasks.size());
        for (Integer i : targetTasks) {
            choices.add(Arrays.asList(i));
            allTasks.add(i);
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        Object last = values.get(values.size() - 1);
        if (last.equals(Constants.EmitFields.EMIT_TO_ALL)) {
            return allTasks;
        }

        String groupField = (String) values.get(0);
        // 为了保证能够将DispatcherDefaultHandler能将schema.table.heartbeat和schema.table,分到相同的task上,这里讲heartbeat后缀截掉
        if (last.equals(Constants.EmitFields.EMIT_HEARTBEAT)) {
            groupField = groupField.substring(0, groupField.length() - Constants.EmitFields.HEARTBEAT_FIELD_SUFFIX.length());
        }
        List<Integer> result = choices.get(Math.abs(groupField.hashCode()) % choices.size());
        return result;
    }
}
