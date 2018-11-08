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

package com.creditease.dbus.stream.dispatcher.helper;

import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.commons.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.PropertyException;
import java.util.Properties;

/**
 * Created by dongwang47 on 2016/8/17.
 */
public class ZKHelper {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String topologyRoot = null;
    private String topologyID = null;
    private String zkServers = null;
    private ZkService zkService = null;

    public ZKHelper(String topologyRoot, String topologyID, String zkServers) throws Exception {
        this.topologyRoot = topologyRoot;
        this.topologyID = topologyID;
        this.zkServers = zkServers;

        zkService = new ZkService(zkServers);
    }

    /**
     * read dbSourceName from ZK and set into DataSourceInfo
     *
     * @param dsInfo (in)
     * @return
     * @throws Exception
     */
    public void loadDsNameAndOffset(DataSourceInfo dsInfo) throws Exception {
        // read dbSourceName
        String path = topologyRoot + "/" + Constants.DISPATCHER_RAW_TOPICS_PROPERTIES;
        Properties raw_topics = zkService.getProperties(path);
        String dbSourceName = raw_topics.getProperty(Constants.DISPATCHER_DBSOURCE_NAME);
        if (dbSourceName == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.DISPATCHER_DBSOURCE_NAME);
        }

        String dataTopicOffset = raw_topics.getProperty(Constants.DISPATCHER_OFFSET);
        if (dataTopicOffset == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.DISPATCHER_OFFSET);
        }

        dsInfo.setDbSourceName(dbSourceName);
        dsInfo.setDataTopicOffset(dataTopicOffset);
    }


    public Properties getProducerProps() throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_PRODUCER_PROPERTIES;
        Properties props = zkService.getProperties(path);
        String clientID = topologyID + "-producer";
        props.put("client.id", clientID);
        return props;
    }

    public Properties getConsumerProps() throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_CUNSUMER_PROPERTIES;
        Properties props = zkService.getProperties(path);
        String id = topologyID + "-consumer";
        props.setProperty("group.id", id);
        props.setProperty("client.id", id);
        logger.info(String.format("Spout set: group.id and client.id=%s", id));
        return props;
    }


    public Properties getStatProducerProps() throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_PRODUCER_PROPERTIES;
        Properties props =  zkService.getProperties(path);
        String clientID = topologyID + "-stat-producer";
        props.put("client.id", clientID);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void saveSchemaProps(Properties schemaTopicProps) throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_SCHEMA_TOPICS_PROPERTIES;
        zkService.setProperties(path, schemaTopicProps);
    }

    /**
     * saveDsInfo
     * Write data topic, control topic and dbus schema to zk.
     * @throws Exception
     */
    public void saveDsInfo(DataSourceInfo dsInfo) throws Exception {
        try {
            String path = topologyRoot + "/" + Constants.DISPATCHER_RAW_TOPICS_PROPERTIES;
            Properties raw_topics = zkService.getProperties(path);
            raw_topics.setProperty(Constants.DISPATCHER_DBSOURCE_NAME, dsInfo.getDbSourceName());
            raw_topics.setProperty(Constants.DISPATCHER_DBSOURCE_TYPE, dsInfo.getDbSourceType());
            raw_topics.setProperty(Constants.DISPATCHER_DATA_TOPIC, dsInfo.getDataTopic());
            raw_topics.setProperty(Constants.DISPATCHER_CTRL_TOPIC, dsInfo.getCtrlTopic());
            raw_topics.setProperty(Constants.DISPATCHER_DBUS_SCHEMA, dsInfo.getDbusSchema());
            raw_topics.setProperty(Constants.DISPATCHER_OFFSET, dsInfo.getDataTopicOffset());
            zkService.setProperties(path, raw_topics);

            logger.info("Successfully save data to raw property file in Zk. ");
        } catch (Exception e) {
            logger.error("Exception caught when writing to zoo keeper" + e.getMessage());
            throw e;
        }
    }

    public void saveReloadStatus(String json, String title, boolean prepare) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkServers);
            sender.send(title, result, prepare, true);
            logger.info(msg);
        }
    }

    public String getStatisticTopic() throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_CONFIG_PROPERTIES;
        Properties configs = zkService.getProperties(path);
        String topic = configs.getProperty(Constants.STATISTIC_TOPIC);
        if (topic == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.STATISTIC_TOPIC);
        }

        return topic;
    }

    public String getSchemaRegistryRestUrl() throws Exception {
        String path = topologyRoot + "/" + Constants.DISPATCHER_CONFIG_PROPERTIES;
        Properties configs = zkService.getProperties(path);
        String topic = configs.getProperty(Constants.SCHEMA_REGISTRY_REST_URL);
        if (topic == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.SCHEMA_REGISTRY_REST_URL);
        }

        return topic;
    }


    public void close() {
        try {
            if (zkService != null) {
                zkService.close();
                zkService = null;
            }
        }catch (Exception ex) {
            logger.error("Close Zookeeper error:" + ex.getMessage());
        }
    }
}
