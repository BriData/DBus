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


package com.creditease.dbus.log.processor.helper;

import com.creditease.dbus.commons.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.creditease.dbus.commons.Constants.LOG_PROCESSOR;


public class ZkHelper {

    private static Logger logger = LoggerFactory.getLogger(ZkHelper.class);

    private String zkStr = null;
    private String topologyId = null;
    private IZkService zkService = null;

    public ZkHelper(String zkStr, String topologyId) {
        this.zkStr = zkStr;
        this.topologyId = topologyId;
        try {
            zkService = new ZkService(zkStr);
        } catch (Exception e) {
            logger.error("ZkHelper init error.", e);
            throw new RuntimeException("ZkHelper init error", e);
        }
    }

    public Properties loadLogProcessorConf() {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/config.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load config.properties error.", e);
            throw new RuntimeException("load config.properties error", e);
        }
    }

    public boolean saveLogProcessorConf(Properties props) {
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/config.properties";
        try {
            logger.info("保存log processor conf 成功！");
            return zkService.setProperties(path, props);
        } catch (Exception e) {
            logger.error("save config.properties error.", e);
            throw new RuntimeException("save config.properties error", e);
        }
    }

    public Properties loadKafkaConsumerConf() {
        Properties consumerProps;
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/consumer.properties";
        try {
            consumerProps = zkService.getProperties(path);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("load consumer.properties error.", e);
            throw new RuntimeException("load consumer.properties error", e);
        }

        Properties props = loadSecurityConf();
        if (props != null) {
            if (StringUtils.equals(props.getProperty("AuthenticationAndAuthorization"), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
                logger.info("consumer security_protocol is enabled!  security_protocol_config is: SASL_PLAINTEXT");
            } else if (StringUtils.equals(props.getProperty("AuthenticationAndAuthorization"), "none")) {
                logger.info("consumer security_protocol is disabled!");
            }
        }

        return consumerProps;
    }

    public Properties loadKafkaProducerConf() {
        Properties producerProps;
        String path = Constants.TOPOLOGY_ROOT + "/" + topologyId + "-" + LOG_PROCESSOR + "/producer.properties";
        try {
            producerProps = zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load producer.properties error.", e);
            throw new RuntimeException("load producer.properties error", e);
        }

        Properties props = loadSecurityConf();
        if (props != null) {
            if (StringUtils.equals(props.getProperty("AuthenticationAndAuthorization"), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
                logger.info("producer security_protocol is enabled!  security_protocol_config is: SASL_PLAINTEXT");
            } else if (StringUtils.equals(props.getProperty("AuthenticationAndAuthorization"), "none")) {
                logger.info("producer security_protocol is disabled!");
            }
        }
        return producerProps;
    }


    private Properties loadSecurityConf() {
        String path = Constants.COMMON_ROOT + "/" + Constants.GLOBAL_SECURITY_CONF;
        try {
            if (zkService.isExists(path)) {
                return zkService.getProperties(path);
            }
            return null;
        } catch (Exception e) {
            logger.error("load global_security.conf error: ", e);
            throw new RuntimeException("load global_security.conf error: ", e);
        }
    }

    public Properties loadMySqlConf() {
        String path = Constants.COMMON_ROOT + "/" + Constants.MYSQL_PROPERTIES;
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load mysql.properties error.", e);
            throw new RuntimeException("load mysql.properties error", e);
        }
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

    public ZkService getZkservice() {
        return (ZkService) zkService;
    }

    public void close() {
        try {
            zkService.close();
        } catch (Exception e) {
            logger.error("close error.", e);
        }
    }
}
