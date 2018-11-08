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

package com.creditease.dbus.commons;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public interface IZkService extends Closeable {

    long SEQUENCE_START = 1;
    long SEQUENCE_STEP = 1000;


    Stat getStat(String path) throws Exception;
    Map<String, Object> getACL(String path) throws Exception;
    boolean isExists(String path) throws Exception;
    int getVersion(String path) throws Exception;

    void createNode(String path, byte[] payload) throws Exception;
    void deleteNode(String path) throws Exception;

    byte[] getData(String path) throws Exception;
    boolean setData(String path, byte[] payload) throws Exception;
    int setDataWithVersion(String path, byte[] payload, int version) throws Exception;

    List<String> getChildren(String path) throws Exception;

    Properties getProperties(String path) throws Exception;
    boolean setProperties(String path, Properties props) throws Exception;

    long nextValue(String dbName, String schemaName, String tableName, String version) throws Exception;
    long nextValue(String nameSpace) throws Exception;

    long currentValue(String dbName, String schemaName, String tableName, String version) throws Exception;
    long currentValue(String nameSpace) throws Exception;

    byte[] registerWatcher(String node, Watcher watcher) throws Exception;

    void rmr(String path) throws Exception;

}
