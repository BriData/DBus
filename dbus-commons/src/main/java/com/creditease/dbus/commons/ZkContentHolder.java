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

import com.creditease.dbus.commons.exception.UnintializedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by Shrimp on 16/6/6.
 */
public class ZkContentHolder {
    private static Logger logger = LoggerFactory.getLogger(ZkContentHolder.class);

    private static ThreadLocal<InnerHolder> innerHolder = new ThreadLocal<>();

    /**
     * 通过zookeeper初始化Constants
     *
     * @param zookeeper zookeeper url字符串
     * @param path      配置保存的根目录,配置文件必须中心化,即所有的配置文件均放置在path下
     * @throws Exception
     */
    public static void initialize(String zookeeper, String path) throws Exception {
        innerHolder.set(new InnerHolder(zookeeper, path));
    }

    /**
     * 获取指定配置文件中 key 对应的value
     *
     * @param config 配置文件名称
     * @param key    配置项的key值
     * @return 返回key对应的value
     */
    public static String getProperties(String config, String key) {
        try {
            String result = null;
            String zkContent = getZkContent(config);
            zkContent = StringUtils.replace(zkContent, "\r", "");
            for (String line : StringUtils.split(zkContent, "\n")) {
                line = line.trim();
                if (StringUtils.isBlank(line) || line.startsWith("#") || line.startsWith("!")) {
                    continue;
                } else if (!line.contains("=") && !line.contains(":")) {
                    continue;
                } else {
                    String escape = getEscape(line);
                    String[] split = StringUtils.split(line, escape, 2);
                    if (split[0].trim().equals(key)) {
                        result = split.length == 2 ? split[1] : null;
                    }
                }
            }
            return StringUtils.trim(result);
        } catch (Exception e) {
            throw new RuntimeException("get zk properties exception!", e);
        }
    }

    public static void setProperties(String config, String key, String value) {
        try {
            String zkContent = getZkContent(config);
            if (StringUtils.isBlank(zkContent)) {
                setZkContent(config, String.format("%s%s%s", key, "=", value));
                return;
            }
            StringBuilder sb = new StringBuilder();
            zkContent = StringUtils.replace(zkContent, "\r", "");
            boolean flag = false;
            String kv = String.format("%s=%s", key, value);
            for (String line : zkContent.split("\n")) {
                if (StringUtils.isBlank(line) || line.startsWith("#") || line.startsWith("!")) {
                    sb.append(line).append("\n");
                    continue;
                } else if (!line.contains("=") && !line.contains(":")) {
                    sb.append(line).append("\n");
                    continue;
                } else {
                    line = line.trim();
                    String escape = getEscape(line);
                    String[] split = StringUtils.split(line, escape, 2);
                    if (split[0].trim().equals(key)) {
                        line = String.format("%s%s%s", key, escape, value);
                        flag = true;
                    }
                    sb.append(line).append("\n");
                }
            }
            if (!flag) {
                sb.append(kv).append("\n");
            }
            setZkContent(config, sb.toString());
        } catch (Exception e) {
            throw new RuntimeException("set zk properties exception!", e);
        }
    }

    private static String getEscape(String line) {
        String escape = "=";
        if (line.contains(":") && (line.indexOf("=") > line.indexOf(":"))) {
            escape = ":";
        }
        return escape;
    }

    private static String getZkContent(String config) throws Exception {
        return innerHolder.get().getContent(config);
    }

    private static void setZkContent(String config, String content) throws Exception {
        innerHolder.get().setContent(config, content);
    }

    /**
     * 重新加载配置文件
     *
     * @throws Exception
     */
    public static void reload() {
        innerHolder.get().reload();
        logger.info("PropertiesHolder has ready to be reloaded");
    }

    private static class InnerHolder {
        private ZkContentReaderAndWriter readerAndWriter;
        private ConcurrentMap<String, String> propMap;

        public InnerHolder(String zk, String path) {
            propMap = new ConcurrentHashMap<>();
            try {
                readerAndWriter = new ZkContentReaderAndWriter(zk, path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void reload() {
            propMap.clear();
        }

        /**
         * 获取指定配置文件的String对象
         *
         * @param config 配置文件名称,不加扩展名
         * @return 返回配置文件内容
         */
        public String getContent(String config) throws Exception {
            if (propMap.containsKey(config)) {
                return propMap.get(config);
            } else {
                if (readerAndWriter == null) {
                    throw new UnintializedException("PropertiesHolder has not initialized");
                }
                String content = readerAndWriter.readZkContent(config);
                if (content != null) {
                    propMap.putIfAbsent(config, content);
                    return content;
                }
            }
            return null;
        }

        /**
         * 更新指定配置文件的String对象
         *
         * @param config  配置文件名称,不加扩展名
         * @param content
         * @throws Exception
         */
        public void setContent(String config, String content) throws Exception {
            readerAndWriter.writeZkContent(config, content);
            // 更新数据到缓存
            propMap.put(config, content);
        }
    }

}
