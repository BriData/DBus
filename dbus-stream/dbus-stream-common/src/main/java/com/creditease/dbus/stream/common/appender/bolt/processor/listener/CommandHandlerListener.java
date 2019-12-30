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


package com.creditease.dbus.stream.common.appender.bolt.processor.listener;

import com.creditease.dbus.commons.ZkService;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

/**
 * Created by Shrimp on 16/7/1.
 */
public interface CommandHandlerListener {
    OutputCollector getOutputCollector();

    void reloadBolt(Tuple tuple);

    default String getZkconnect() {
        return null;
    }

    default ZkService getZkService() {
        return null;
    }

    default void reloadZkService() {

    }
}
