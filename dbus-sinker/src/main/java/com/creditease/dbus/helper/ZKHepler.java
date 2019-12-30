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

import com.creditease.dbus.commons.*;
import com.creditease.dbus.tools.SinkerConstants;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Properties;


public class ZKHepler {

    private Logger logger = LoggerFactory.getLogger(ZKHepler.class);
    private String sinkerRootPath = null;

    private String zkStr = null;
    private IZkService zkService = null;

    public ZKHepler(String zkStr, String topologyId) throws Exception {
        this.zkStr = zkStr;
        this.zkService = new ZkService(zkStr);
        this.sinkerRootPath = Constants.SINKER_ROOT + "/" + topologyId + "-sinker";
    }

    public Properties loadSinkerConf(String confName) throws Exception {
        String path = sinkerRootPath + "/" + confName;
        try {
            Properties properties = zkService.getProperties(path);
            if (confName.equals(SinkerConstants.CONSUMER) || confName.equals(SinkerConstants.PRODUCER)) {
                Properties globalProp = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
                properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, globalProp.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
                if (isSecurityConf()) {
                    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
                }
            }
            logger.info("load zk properties -- {} : {}", path, properties);
            return properties;
        } catch (Exception e) {
            logger.error("load sinker zk node [{}] properties error.", confName, e);
            throw e;
        }
    }

    public Properties getProperties(String path) throws Exception {
        return zkService.getProperties(path);
    }

    public void saveReloadStatus(String json, String title, boolean prepare) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkStr);
            sender.send(title, result, prepare, false);
        }
    }

    public void createNode(String path) {
        try {
            if (zkService.isExists(path)) {
                logger.info("zk path [{}] already exists.", path);
                zkService.setData(path, new byte[0]);
            } else {
                zkService.createNode(path, new byte[0]);
                logger.info("create zk path [{}] success.", path);
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("create zk path [{}] error.", path), e);
        }
    }

    public void deleteNode(String path) {
        try {
            if (zkService.isExists(path)) {
                zkService.deleteNode(path);
                logger.info("delete zk path [{}] success.", path);
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("delete zk path [{}] error.", path), e);
        }
    }

    public void setData(String path, byte[] data) {
        try {
            if (zkService.isExists(path)) {
                zkService.setData(path, data);
            } else {
                logger.warn("zk node [{}] not exit don't set data.", path);
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("set data for path [{}] error.", path), e);
        }
    }

    public byte[] getData(String path) {
        byte[] data = null;
        try {
            if (zkService.isExists(path)) {
                data = zkService.getData(path);
            } else {
                logger.warn("zk node [{}] not exit don't get data.", path);
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("get data for path [{}] error.", path), e);
        }
        return data;
    }

    public boolean isSecurityConf() {
        String path = Constants.COMMON_ROOT + "/" + Constants.GLOBAL_SECURITY_CONF;

        try {
            if (zkService.isExists(path)) {
                Properties properties = zkService.getProperties(path);
                String security = properties.getProperty("AuthenticationAndAuthorization");
                if (Constants.SECURITY_CONFIG_TRUE_VALUE.equals(security)) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("get security config for path [{}] error.", path), e);
        }
        return false;
    }

    public void close() {
        try {
            zkService.close();
        } catch (Exception e) {
            logger.error("close error.", e);
        }
    }

}
