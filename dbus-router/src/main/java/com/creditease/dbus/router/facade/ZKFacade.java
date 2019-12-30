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


package com.creditease.dbus.router.facade;

import com.creditease.dbus.commons.*;
import com.creditease.dbus.router.util.DBusRouterConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.MessageFormat;
import java.util.Properties;

import static com.creditease.dbus.commons.Constants.ROUTER;

/**
 * Created by mal on 2018/5/22.
 */
public class ZKFacade {

    private static Logger logger = LoggerFactory.getLogger(ZKFacade.class);

    private String zkStr = null;
    private String topologyId = null;
    private String projectName = null;
    private IZkService zkService = null;

    public ZKFacade(String zkStr, String topologyId, String projectName) throws Exception {
        this.zkStr = zkStr;
        this.topologyId = topologyId;
        this.projectName = projectName;
        zkService = new ZkService(zkStr);
    }

    public Properties loadRouterConf() throws Exception {
        String path = Constants.ROUTER_ROOT + "/" + projectName + "/" + topologyId + "-" + ROUTER + "/config.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load dbus router config.properties error.", e);
            throw e;
        }
    }

    public String resetOffset() throws Exception {
        ByteArrayInputStream bais = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        ByteArrayOutputStream bros = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        String retVal = null;
        String path = Constants.ROUTER_ROOT + "/" + projectName + "/" + topologyId + "-" + ROUTER + "/config.properties";
        try {
            byte[] data = zkService.getData(path);
            bais = new ByteArrayInputStream(data);
            isr = new InputStreamReader(bais);
            br = new BufferedReader(isr);

            bros = new ByteArrayOutputStream(data.length);
            osw = new OutputStreamWriter(bros);
            bw = new BufferedWriter(osw);
            String line = br.readLine();
            while (line != null) {
                if (StringUtils.contains(line, DBusRouterConstants.TOPIC_OFFSET)) {
                    String arrs[] = StringUtils.split(line, "=");
                    line = StringUtils.replace(line, arrs[1], "none");
                }
                bw.write(line);
                bw.newLine();
                line = br.readLine();
            }
            bw.flush();
            retVal = bros.toString();
            zkService.setData(path, bros.toByteArray());
        } catch (Exception e) {
            logger.error("save dbus router config.properties error.", e);
            throw e;
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(bros);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(bais);
        }
        return retVal;
    }

    public Properties loadKafkaConsumerConf() throws Exception {
        String path = Constants.ROUTER_ROOT + "/" + projectName + "/" + topologyId + "-" + ROUTER + "/consumer.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load dbus router consumer.properties error.", e);
            throw e;
        }
    }

    public Properties loadKafkaProducerConf() throws Exception {
        String path = Constants.ROUTER_ROOT + "/" + projectName + "/" + topologyId + "-" + ROUTER + "/producer.properties";
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load dbus router producer.properties error.", e);
            throw e;
        }
    }

    public Properties loadMySqlConf() throws Exception {
        String path = Constants.COMMON_ROOT + "/" + Constants.MYSQL_PROPERTIES;
        try {
            return zkService.getProperties(path);
        } catch (Exception e) {
            logger.error("load mysql.properties error.", e);
            throw e;
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

    public String getSecurityConf() {
        String security = "none";
        String path = Constants.COMMON_ROOT + "/" + Constants.GLOBAL_SECURITY_CONF;
        try {
            if (zkService.isExists(path)) {
                Properties properties = zkService.getProperties(path);
                security = properties.getProperty("AuthenticationAndAuthorization");
            }
        } catch (Exception e) {
            logger.error(MessageFormat.format("get security config for path [{}] error.", path), e);
        }
        return security;
    }

    public void close() {
        try {
            zkService.close();
        } catch (Exception e) {
            logger.error("close error.", e);
        }
    }

}
