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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by Shrimp on 16/6/6.
 */
public class PropertiesHolder {
    private static Logger logger = LoggerFactory.getLogger(PropertiesHolder.class);

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
     * 获取指定配置文件的Properties对象
     *
     * @param config 配置文件名称,不加扩展名
     * @return 返回配置文件内容
     */
    public static Properties getProperties(String config) throws Exception {
        return innerHolder.get().getProperties(config);
    }

    /**
     * 重载的方法
     * 获取指定配置文件的Properties对象
     *
     * @param confFileName      配置文件名称,不加扩展名
     * @param customizeConfPath 每个业务线的定制化配置路径，如果有的话。（config存储基础配置信息。）
     * @return 返回配置文件内容
     */
    public static Properties getPropertiesInculdeCustomizeConf(String confFileName, String customizeConfPath, boolean useCacheIfCached) throws Exception {
        return innerHolder.get().getPropertiesInculdeCustomizeConf(confFileName, customizeConfPath, useCacheIfCached);
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
            Object val = getProperties(config).get(key);
            if (val != null) {
                return val.toString().trim();
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Properties File not found!", e);
        }
    }

    public static Integer getIntegerValue(String config, String key) {
        String value = getProperties(config, key);
        if (value != null && value.length() > 0) {
            return Integer.parseInt(value);
        }
        return null;
    }

    public static Long getLongValue(String config, String key) {
        String value = getProperties(config, key);
        if (value != null && value.length() > 0) {
            return Long.parseLong(value);
        }
        return null;
    }

    /**
     * 重新加载配置文件
     *
     * @throws Exception
     */
    public static void reload() throws Exception {
        innerHolder.get().reload();
        logger.info("PropertiesHolder has ready to be reloaded");
    }

    private static class InnerHolder {
        private PropertiesProvider provider;
        private ConcurrentMap<String, Properties> propMap;

        public InnerHolder(String zk, String path) {
            propMap = new ConcurrentHashMap<>();
            try {
                provider = new ZkPropertiesProvider(zk, path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void reload() {
            propMap.clear();
        }

        /**
         * 获取指定配置文件的Properties对象
         *
         * @param config 配置文件名称,不加扩展名
         * @return 返回配置文件内容
         */
        public Properties getProperties(String config) throws Exception {
            if (propMap.containsKey(config)) {
                return propMap.get(config);
            } else {
                if (provider == null) {
                    throw new UnintializedException("PropertiesHolder has not initialized");
                }
                Properties properties = provider.loadProperties(config);
                if (properties != null) {
                    propMap.putIfAbsent(config, properties);
                    return properties;
                }
            }
            return null;
        }

        /**
         * 获取指定配置文件的Properties对象
         *
         * @param confFileName      配置文件名称,不加扩展名
         * @param customizeConfPath TODO
         * @return 返回配置文件内容
         */
        public Properties getPropertiesInculdeCustomizeConf(String confFileName, String customizeConfPath, boolean useCacheIfCached) throws Exception {
            if (propMap.containsKey(confFileName) && useCacheIfCached) {
                return propMap.get(confFileName);
            } else {
                if (provider == null) {
                    throw new UnintializedException("PropertiesHolder has not initialized");
                }
                Properties properties = provider.loadProperties(confFileName);

                if (null != customizeConfPath && !"".equals(customizeConfPath)) {
                    Properties customizeProperties = provider.loadProperties(customizeConfPath);
                    properties.putAll(customizeProperties);
                }

                if (properties != null) {
                    propMap.putIfAbsent(confFileName, properties);
                    return properties;
                }
            }
            return null;
        }
    }

    public static void main(String[] args) {
        try {
            //localInitialize();
            initialize("localhost:2181", "/DBus/Topology/dbus-ora-appender-01");
            reload();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
