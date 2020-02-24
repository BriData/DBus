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


package com.creditease.dbus.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载zookeeper配置中心中的配置文件
 * Created by Shrimp on 16/6/6.
 */
public class ZkContentReaderAndWriter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private String EXT_PATTERN = ".properties";
    private IZkService zookeeper;
    private String path;
    private String url;

    public ZkContentReaderAndWriter(String url, String path) throws Exception {
        this.url = url;
        this.path = path;
        zookeeper = new ZkService(url, 30000);
    }

    public String readZkContent(String node) throws Exception {
        if (path == null) {
            throw new IllegalArgumentException();
        }
        String nodePath = buildPath(node);
        if (zookeeper.isExists(nodePath)) {
            byte[] data = zookeeper.getData(nodePath);
            return data != null && data.length > 0 ? new String(data) : null;
        } else {
            return null;
        }
    }

    public void writeZkContent(String node, String content) throws Exception {
        if (path == null) {
            throw new IllegalArgumentException();
        }
        String nodePath = buildPath(node);
        if (zookeeper.isExists(nodePath)) {
            zookeeper.setData(nodePath, content.getBytes());
        } else {
            throw new RuntimeException("zookeeper node not exists exception");
        }
    }

    private String buildPath(String node) {
        String nodePath;
        if (node.startsWith("/")) {
            nodePath = node;
        } else {
            nodePath = path + "/" + node;
        }
        if (!nodePath.endsWith(EXT_PATTERN)) {
            nodePath += EXT_PATTERN;
        }
        return nodePath;
    }

}
