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


package com.creditease.dbus.extractor.common.utils;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.extractor.container.DataSourceContainer;
import com.creditease.dbus.extractor.container.ExtractorConfigContainer;
import com.creditease.dbus.extractor.container.TableMatchContainer;
import com.creditease.dbus.extractor.dao.ILoadDbusConfigDao;
import com.creditease.dbus.extractor.dao.impl.LoadDbusConfigDaoImpl;
import com.creditease.dbus.extractor.vo.ExtractorVo;
import com.creditease.dbus.extractor.vo.JdbcVo;
import com.creditease.dbus.extractor.vo.OutputTopicVo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class ZKHelper {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String zkServers = null;
    private ZkService zkService = null;
    private String extractorRoot = null;
    private String extractorName = null;

    public ZKHelper(String zkServers, String extractorRoot, String extractorName) throws Exception {
        this.zkServers = zkServers;
        this.extractorRoot = extractorRoot;
        this.extractorName = extractorName;
        //zkService = new ZkService(this.zkServers);
        try {
            zkService = new ZkService(this.zkServers);
        } catch (Exception e) {
            logger.error("Create new zkservice failed. Exception:", e);
        }
    }

    public void loadJdbcConfig() throws Exception {
        //TODO
        String path = extractorRoot + extractorName + "/jdbc.properties";
        Properties jdbcConfigs = zkService.getProperties(path);
        Properties authProps = zkService.getProperties(Constants.COMMON_ROOT + '/' + Constants.MYSQL_PROPERTIES);
        List<JdbcVo> jdbc = jdbcConfigParse(jdbcConfigs, authProps);  //TODO 后期将返回JdbcVo List改为返回JdbcVo,貌似不需要List即可
        ExtractorConfigContainer.getInstances().setJdbc(jdbc);
        for (JdbcVo conf : jdbc)
            DataSourceContainer.getInstances().register(conf);
        logger.info("jdbc.properties:{}", jdbcConfigs);
    }

    public void loadOutputTopic() {
        ILoadDbusConfigDao dbusDao = new LoadDbusConfigDaoImpl();
        Set<OutputTopicVo> topics = dbusDao.queryOutputTopic(Constants.CONFIG_DB_KEY);
        ExtractorConfigContainer.getInstances().setOutputTopic(topics);
        //ExtractorConfigContainer.getInstances().setControlTopic();//todo 参数
        logger.info("output topics:{}", topics);
    }

    //此处的tableregex是从zookeeper的config.property中读取的，要改 20180307 review
    public void loadExtractorConifg() {
        //TODO
        String path = extractorRoot + extractorName + "/config.properties";
        try {
            Properties extractorConfigs = zkService.getProperties(path);
            ExtractorVo extVo = extractorConfigParse(extractorConfigs);
            ExtractorConfigContainer.getInstances().setExtractorConfig(extVo);
            // TableMatchContainer.getInstance().addTableRegex(extVo.getPartitionTableRegex());
            logger.info("extractor configs:{}", extVo);
        } catch (Exception e) {
            //logger.info(e.getMessage());
            throw new RuntimeException("load extractor config: " + path + "配置信息出错.", e);
        }
    }

    public void loadKafkaProducerConfig() {
        //TODO
        String path = extractorRoot + extractorName + "/producer.properties";
        try {
            Properties kafkaProducerConfigs = zkService.getProperties(path);
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                kafkaProducerConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            ExtractorConfigContainer.getInstances().setKafkaProducerConfig(kafkaProducerConfigs);
            logger.info("kafka producer configs:{}", kafkaProducerConfigs);
        } catch (Exception e) {
            throw new RuntimeException("load extractor config: " + path + "配置信息出错.", e);
        }
    }

    public void loadKafkaConsumerConfig() {
        //TODO
        String path = extractorRoot + extractorName + "/consumer.properties";
        try {
            Properties kafkaConsumerConfigs = zkService.getProperties(path);
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                kafkaConsumerConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            ExtractorConfigContainer.getInstances().setKafkaConsumerConfig(kafkaConsumerConfigs);
            logger.info("kafka consumer configs:{}", kafkaConsumerConfigs);
        } catch (Exception e) {
            throw new RuntimeException("load extractor config: " + path + "配置信息出错.", e);
        }
    }

    public void loadFilter() {
        ILoadDbusConfigDao dbusDao = new LoadDbusConfigDaoImpl();

        String dsName = ExtractorConfigContainer.getInstances().getExtractorConfig().getDbName();
        List<String> ret = dbusDao.queryActiveTable(dsName, Constants.CONFIG_DB_KEY);
        String filter = ret.get(0);
        ExtractorConfigContainer.getInstances().setFilter(filter);
        TableMatchContainer.getInstance().addTableRegex(ret.get(1));

        logger.info("active tables filter is :{}", filter);
        //存储到zookeeper中
        String path = extractorRoot + extractorName + "/filter.properties";
        try {
            if (!zkService.isExists(path)) {
                zkService.createNode(path, null);
            }
            Properties filterProp = new Properties();
            filterProp.setProperty("", filter);
            zkService.setData(path, filter.getBytes());
            logger.info("saved filter information to zookeeper, path is {}, filter is {}", path, filter);
        } catch (Exception e) {
            logger.info("save filter info into zookeeper exception，{}", e);
        }
    }

    //写reload的状态到zookeeper中/DBus/ControlMessageResult/EXTRACTOR_RELOAD_CONFIG
    public void saveReloadStatus(String json, String title, boolean prepare) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            String parentNode = message.getType();
            //CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkServers);
            CtlMessageResultSender sender = new CtlMessageResultSender(parentNode, zkServers);
            sender.send(title, result, prepare, true);
            logger.info(msg);
        }
    }

    public void close() {
        try {
            if (zkService != null) {
                zkService.close();
                zkService = null;
                System.out.println("close zookeeper connect");
            }
        } catch (Exception ex) {
            logger.error("Close Zookeeper error:" + ex.getMessage());
        }
    }

    private List<JdbcVo> jdbcConfigParse(Properties prop, Properties authProps) {
        List<JdbcVo> confs = new ArrayList<JdbcVo>();
        try {
            JdbcVo conf = new JdbcVo();
            conf.setDriverClass(prop.getProperty("DB_DRIVER_CLASS"));
            conf.setInitialSize(Integer.parseInt(prop.getProperty("DS_INITIAL_SIZE")));
            conf.setMaxActive(Integer.parseInt(prop.getProperty("DS_MAX_ACTIVE")));
            conf.setMaxIdle(Integer.parseInt(prop.getProperty("DS_MAX_IDLE")));
            conf.setMinIdle(Integer.parseInt(prop.getProperty("DS_MIN_IDLE")));
            conf.setType(prop.getProperty("DB_TYPE"));
            conf.setKey(prop.getProperty("DB_KEY"));
            conf.setUrl(authProps.getProperty("url"));
            conf.setUserName(authProps.getProperty("username"));
            conf.setPassword(authProps.getProperty("password"));
            confs.add(conf);
            System.out.println("jdbc.properties loaded");
        } catch (Exception e) {
            logger.info("parse config resource " + "jdbc.properties" + " error!");
            //throw new RuntimeException("parse config resource " + "jdbc.properties" + " error!");
        }
        return confs;
    }

    private ExtractorVo extractorConfigParse(Properties prop) {
        //TODO
        ExtractorVo extVo = new ExtractorVo();
        try {
            extVo.setCanalInstanceName(prop.getProperty("canal.instance.name"));
            extVo.setDbType(prop.getProperty("database.type"));
            extVo.setDbName(prop.getProperty("database.name"));
            extVo.setCanalBatchSize(Integer.parseInt(prop.getProperty("canal.client.batch.size")));
            extVo.setSubscribeFilter(prop.getProperty("canal.client.subscribe.filter"));
            extVo.setCanalZkPath(prop.getProperty("canal.zk.path"));
            extVo.setKafkaSendBatchSize(Integer.parseInt(prop.getProperty("kafka.send.batch.size")));
            extVo.setPartitionTableRegex(prop.getProperty("table.partition.regex"));
        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + "config.properties" + " error!");
        }
        return extVo;
    }

}
