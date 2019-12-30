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


package com.creditease.dbus.helper;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.PropertiesProvider;
import com.creditease.dbus.commons.ZkPropertiesProvider;
import com.creditease.dbus.commons.exception.UnintializedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by Shrimp on 16/6/6.
 */
public class FullPullPropertiesHolder {
    private static Logger logger = LoggerFactory.getLogger(FullPullPropertiesHolder.class);

    private static ThreadLocal<String> splitterPathHolder = new ThreadLocal<>();
    private static ThreadLocal<String> pullerPathHolder = new ThreadLocal<>();

    private static ThreadLocal<InnerHolder> splitterHolder = new ThreadLocal<>();
    private static ThreadLocal<InnerHolder> pullerHolder = new ThreadLocal<>();

    public static String getPath(String topoType) {
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            return splitterPathHolder.get();
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            return pullerPathHolder.get();
        } else {
            throw new RuntimeException("getPath(): bad topology type!! " + topoType);
        }
    }

    /**
     * 通过zookeeper初始化Constants
     *
     * @param zookeeper zookeeper url字符串
     * @param path      配置保存的根目录,配置文件必须中心化,即所有的配置文件均放置在path下
     * @throws Exception
     */
    public static void initialize(String topoType, String zookeeper, String path) throws Exception {
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            splitterHolder.set(new InnerHolder(zookeeper, path));
            splitterPathHolder.set(path);
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            pullerHolder.set(new InnerHolder(zookeeper, path));
            pullerPathHolder.set(path);
        } else {
            throw new RuntimeException("initialize(): bad topology type!! " + topoType);
        }
    }

    /**
     * 获取指定配置文件的Properties对象
     *
     * @param config 配置文件名称,不加扩展名
     * @return 返回配置文件内容
     */
    public static Properties getProperties(String topoType, String config) throws Exception {
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            return splitterHolder.get().getProperties(config);
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            return pullerHolder.get().getProperties(config);
        } else {
            throw new RuntimeException("getProperties(): bad topology type!! " + topoType);
        }
    }

    /**
     * 重载的方法
     * 获取指定配置文件的Properties对象
     *
     * @param confFileName      配置文件名称,不加扩展名
     * @param customizeConfPath 每个业务线的定制化配置路径，如果有的话。（config存储基础配置信息。）
     * @return 返回配置文件内容
     */
    public static Properties getPropertiesInculdeCustomizeConf(String topoType, String confFileName, String customizeConfPath, boolean useCacheIfCached) throws Exception {
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            return splitterHolder.get().getPropertiesInculdeCustomizeConf(confFileName, customizeConfPath, useCacheIfCached);
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            return pullerHolder.get().getPropertiesInculdeCustomizeConf(confFileName, customizeConfPath, useCacheIfCached);
        } else {
            throw new RuntimeException("getPropertiesInculdeCustomizeConf(): bad topology type!! " + topoType);
        }
    }

    public static Properties getCommonConf(String topoType, String topologyId) {
        String confFileName = FullPullConstants.COMMON_CONFIG;
        String customizeConfPath = topologyId + "/" + confFileName;
        logger.info("getCommonConf(). customizeConfPath is {}", customizeConfPath);
        try {
            Properties properties = getPropertiesInculdeCustomizeConf(topoType, confFileName, customizeConfPath, true);
            return properties;
        } catch (Exception e) {
            return new Properties();
        }
    }

    public static Properties getGlobalConf(String topoType) {
        String customizeConfPath = Constants.GLOBAL_PROPERTIES_ROOT;
        logger.info("getGlobalConf(). customizeConfPath is {}", customizeConfPath);
        try {
            return FullPullPropertiesHolder.getProperties(topoType, customizeConfPath);
        } catch (Exception e) {
            return new Properties();
        }
    }

    private static class InnerHolder {
        private PropertiesProvider provider;
        private ConcurrentMap<String, Properties> propMap;

        public InnerHolder(String zk, String path) {
            propMap = new ConcurrentHashMap<>();
            try {
                provider = new ZkPropertiesProvider(zk, path);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
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
                    throw new UnintializedException("FullPullPropertiesHolder has not initialized");
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
                    throw new UnintializedException("FullPullPropertiesHolder has not initialized");
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
//            initialize("localhost:2181", "/DBus/Topology/dbus-ora-appender-01");
//            reload();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
