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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 加载zookeeper配置中心中的配置文件
 * Created by Shrimp on 16/6/6.
 */
public class ZkPropertiesProvider implements PropertiesProvider {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private IZkService zookeeper;
    private String path;
    private String url;
    private static Boolean isSecurity = null;

    public ZkPropertiesProvider(String url, String path) throws Exception {
        this.url = url;
        this.path = path;
        zookeeper = new ZkService(url, 30000);
    }


    @Override
    public Properties loadProperties(String node) throws Exception {
        if (path == null) {
            throw new IllegalArgumentException();
        }
        String nodePath = buildPath(node);
        if (zookeeper.isExists(nodePath)) {
            Properties properties = zookeeper.getProperties(nodePath);
            if (node.contains("consumer-config") || node.contains("producer-config") || node.contains("producer-control")) {
                addSecurityConf(properties);
            }
            listValues(properties, nodePath);
            return properties;
        } else {
            return new Properties();
        }
    }

    private void addSecurityConf(Properties properties) throws Exception {
        if (isSecurity == null) {
            synchronized (this.getClass()) {
                if (isSecurity == null) {
                    String path = Constants.COMMON_ROOT + "/" + Constants.GLOBAL_SECURITY_CONF;
                    if (zookeeper.isExists(path)) {
                        Properties pro = zookeeper.getProperties(path);
                        if (StringUtils.equals(pro.getProperty("AuthenticationAndAuthorization"), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                            isSecurity = true;
                        } else {
                            isSecurity = false;
                        }
                    } else {
                        isSecurity = false;
                    }
                }
            }
        }
        if (isSecurity) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
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

    private void listValues(Properties properties, String path) {
        logger.info("properties:{}, values:{}", path, properties.toString());

//        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
//            logger.info(String.format("%s=%s", entry.getKey(), entry.getValue().toString()));
//        }
    }
}
