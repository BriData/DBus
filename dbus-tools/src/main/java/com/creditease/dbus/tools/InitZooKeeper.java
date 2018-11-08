/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.tools;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.tools.common.ConfUtils;
import com.creditease.dbus.utils.ZkConfTemplateHelper;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Create the following nodes to Zookeeper:
 */
public class InitZooKeeper {
    private static Logger logger = LoggerFactory.getLogger(InitZooKeeper.class);

    // Keep two zkServices
    private static ZkService myZk = null; // Zk server with security
    private String zkServer;

    private final static String HEARTBEAT_CONFIG_PATH = "InitZooKeeper/" + Constants.HEARTBEAT_JSON;
    private final static String MYSQL_PROPERTIES_PATH = "InitZooKeeper/" + Constants.MYSQL_PROPERTIES;
    private final static String GLOBAL_PROPERTIES_PATH = "InitZooKeeper/" + Constants.GLOBAL_PROPERTIES;

    private String kafkaBootstrapServers;


    public static final String DBUS_CONF_FILES_ROOT = "InitZooKeeper/templates/";
    public static final String DS_NAME_PLACEHOLDER = "placeholder";
    public static final String BOOTSTRAP_SERVER_PLACEHOLDER = "[BOOTSTRAP_SERVER_PLACEHOLDER]";
    public static final String ZK_SERVER_PLACEHOLDER = "[ZK_SERVER_PLACEHOLDER]";


    public static void main(String[] args) throws Exception {
        InitZooKeeper izk = new InitZooKeeper();
        izk.initParams();
        try {
            // 如果通过参数传入了数据线名称，直接为具体数据线上载zk 配置。
            if (args != null && args.length == 1) {
                izk.initializeDsZkConf(args[0]);
            } else {
                // 未传入任何参数，初始化zk及conftemplate
                izk.initializeZooKeeper();
            }
        } catch (Exception e) {
            logger.error("Exception caught when initializing Zookeeper!");
            e.printStackTrace();
            throw e;
        } finally {
            logger.info("Successfully added new nodes on ZooKeeper. ");
            // Close zk instance
            if (myZk != null) {
                myZk.close();
            }
        }
    }

    public void initParams() throws Exception {
        // Parse zookeeper
        Properties props = ConfUtils.loadZKProperties();
        zkServer = props.getProperty(Constants.ZOOKEEPER_SERVERS);
        kafkaBootstrapServers = props.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS);
        if (Strings.isNullOrEmpty(zkServer)) {
            logger.error("Zookeeper server cannot be null!");
            return;
        }
        if (Strings.isNullOrEmpty(kafkaBootstrapServers)) {
            logger.error("kafka server cannot be null!");
            return;
        }

        // Connect to ZooKeeper securely with Auth info
        this.myZk = new ZkService(zkServer);
    }

    public void initializeZooKeeper() throws Exception {

        //创建跟目录
        insertNodeWithCheckDup(Constants.DBUS_ROOT, null);

        //创建一级目录
        insertNodeWithCheckDup(Constants.COMMON_ROOT, null);
        this.uploadCommonProperties();

        //上传模板文件
        insertNodeWithCheckDup(Constants.DBUS_CONF_TEMPLATE_ROOT, null);
        this.uploadTemplate();

        insertNodeWithCheckDup(Constants.CONTROL_MESSAGE_RESULT_ROOT, null);

        insertNodeWithCheckDup(Constants.EXTRACTOR_ROOT, null);

        insertNodeWithCheckDup(Constants.FULL_PULL_MONITOR_ROOT, null);
        insertNodeWithCheckDup(Constants.FULL_PULL_MONITOR_ROOT_GLOBAL, null);

        insertNodeWithCheckDup(Constants.HEARTBEAT_ROOT, null);
        insertNodeWithCheckDup(Constants.HEARTBEAT_CONFIG, null);
        byte[] heartBeatConfData = ConfUtils.toByteArray(HEARTBEAT_CONFIG_PATH);
        insertNodeWithCheckDup(Constants.HEARTBEAT_CONFIG + "/" + Constants.HEARTBEAT_JSON, heartBeatConfData);

        insertNodeWithCheckDup(Constants.NAMESPACE_ROOT, null);

        insertNodeWithCheckDup(Constants.TOPOLOGY_ROOT, null);

        insertNodeWithCheckDup(Constants.STREAMING_ROOT, null);

        //创建其他canal目录
        insertNodeWithCheckDup(Constants.CANAL_ROOT, null);
    }

    /**
     * Insert a node with checking duplicates. If
     * there is already a node there, we do not insert
     * in order to avoid errors.
     */
    private void insertNodeWithCheckDup(String path, byte[] data) throws Exception {
        try {
            if (!myZk.isExists(path)) {
                myZk.createNode(path, data);
                logger.info(String.format("create node '%s' OK!", path));
            } else {
                logger.warn(String.format("Node %s already exists. ", path));
            }
        } catch (Exception e) {
            logger.error("Exception caught when creating a node %s", path);
            e.printStackTrace();
            throw e;
        }
    }

    private void uploadCommonProperties() throws Exception {
        String globalPropPath = Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES;
        byte[] globalPropData = ConfUtils.toByteArray(GLOBAL_PROPERTIES_PATH);
//        if (myZk.isExists(globalPropPath)) {
//            myZk.deleteNode(globalPropPath);
//        }
        String fileContent = new String(globalPropData);
        fileContent = fileContent.replace(BOOTSTRAP_SERVER_PLACEHOLDER, kafkaBootstrapServers);
        globalPropData = fileContent.getBytes();

        insertNodeWithCheckDup(globalPropPath, globalPropData);
        logger.info(String.format("create node '%s' OK!", globalPropPath));

        String sqlPropPath = Constants.COMMON_ROOT + "/" + Constants.MYSQL_PROPERTIES;
        if (myZk.isExists(sqlPropPath)) {
            myZk.deleteNode(sqlPropPath);
        }
        // mysql property needs to be created with security
        byte[] sqlPropData = ConfUtils.toByteArray(MYSQL_PROPERTIES_PATH);
        myZk.createNodeWithACL(sqlPropPath, sqlPropData);
        logger.info(String.format("create node '%s' OK!", sqlPropPath));
    }

    private void uploadTemplate() throws Exception {
        for (String confFilePath : ZkConfTemplateHelper.ZK_CONF_PATHS) {
            // 拟添加节点的各级父节点需先生成好
            String[] confFilePathSplitted = confFilePath.split("/");
            int confFilePathPrefixsCount = confFilePathSplitted.length - 1;
            String precedantPath = Constants.DBUS_CONF_TEMPLATE_ROOT;
            for (int i = 0; i < confFilePathPrefixsCount; i++) {
                precedantPath = precedantPath + "/" + confFilePathSplitted[i];
                insertNodeWithCheckDup(precedantPath, null);
            }

            // 将配置模板上传到zk
            String zkPath = Constants.DBUS_CONF_TEMPLATE_ROOT + "/" + confFilePath;
            String fileName = DBUS_CONF_FILES_ROOT + confFilePath;
            byte[] data = ConfUtils.toByteArray(fileName);
            String fileContent = new String(data);
            fileContent = fileContent.replace(BOOTSTRAP_SERVER_PLACEHOLDER, kafkaBootstrapServers);
            fileContent = fileContent.replace(ZK_SERVER_PLACEHOLDER, zkServer);
            data = fileContent.getBytes();
            insertNodeWithCheckDup(zkPath, data);
        }
        logger.info(String.format("Upload properites success!"));
    }

    public void initializeDsZkConf(String dsName) throws Exception {
        insertNodeWithCheckDup(Constants.TOPOLOGY_ROOT, null);
        for (String confFilePath : ZkConfTemplateHelper.ZK_CONF_PATHS) {
            // 拟添加节点的各级父节点需先生成好
            String[] confFilePathSplitted = confFilePath.split("/");
            int confFilePathPrefixsCount = confFilePathSplitted.length - 1;
            String precedantPath = Constants.DBUS_ROOT;
            for (int i = 0; i < confFilePathPrefixsCount; i++) {
                precedantPath = precedantPath + "/" + confFilePathSplitted[i];
                precedantPath = precedantPath.replace(DS_NAME_PLACEHOLDER, dsName);
                insertNodeWithCheckDup(precedantPath, new byte[0]);
            }

            // 将配置模板上传到zk
            String zkPath = precedantPath + "/" + confFilePathSplitted[confFilePathSplitted.length - 1];
            zkPath = zkPath.replace(DS_NAME_PLACEHOLDER, dsName);
            String fileName = DBUS_CONF_FILES_ROOT + confFilePath;
            byte[] data = ConfUtils.toByteArray(fileName);
            String fileContent = new String(data);
            fileContent = fileContent.replace(BOOTSTRAP_SERVER_PLACEHOLDER, kafkaBootstrapServers);
            fileContent = fileContent.replace(ZK_SERVER_PLACEHOLDER, zkServer);
            fileContent = fileContent.replace(DS_NAME_PLACEHOLDER, dsName);
            data = fileContent.getBytes();
            insertNodeWithCheckDup(zkPath, data);
        }
        logger.info(String.format("Upload properites success!"));
    }
}
