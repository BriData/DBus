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


package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.FileUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class DeployPropsFileHandler extends AbstractHandler {
    @Override
    public void checkDeploy(BufferedWriter bw) throws Exception {
        deployProducerProps(bw);
        deployReplicateProps(bw);
    }

    private void deployReplicateProps(BufferedWriter bw) throws Exception {
        try {
            writeAndPrint("********************************* REPLICAT DEPLOY START *************************************");
            ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
            String dsName = config.getDsName();
            //dirprm的目录：如 /u01/golden123111/dirprm/
            String dirprmPath = config.getOggBigHome() + "/dirprm";
            writeAndPrint("replicat进程的配置文件: " + dsName + ".props");

            Properties updateProps = new Properties();
            updateProps.put("gg.handler.kafkahandler.KafkaProducerConfigFile", config.getKafkaProducerName());
            updateProps.put("gg.handler.kafkahandler.topicMappingTemplate", dsName);
            updateProps.put("gg.handler.kafkahandler.schemaTopicName", dsName + "_schema");

            String currentDir = System.getProperty("user.dir");
            FileUtil.WriteProperties(currentDir + "/conf/" + "replicate.properties",
                    updateProps, dirprmPath + "/" + dsName + ".props", bw);

            writeAndPrint("******************************** REPLICAT DEPLOY SUCCESS ************************************");

        } catch (Exception e) {
            writeAndPrint("********************************* REPLICAT DEPLOY FAIL **************************************");
            throw e;
        }


    }

    private void deployProducerProps(BufferedWriter bw) throws Exception {
        try {
            ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
            writeAndPrint("******************************** KAFKA PRODUCER DEPLOY START ********************************");
            writeAndPrint("kafka producer的配置文件: " + config.getKafkaProducerName());

            //dirprm的目录：如 /u01/golden123111/dirprm/
            String dirprmPath = config.getOggBigHome() + "/dirprm";
            File path = new File(dirprmPath);
            if (!path.exists()) {
                writeAndPrint(dirprmPath + "  目录不存在，请检查ogg.big.home 配置是否正确");
                throw new FileNotFoundException();
            }

            Properties updateProps = new Properties();
            updateProps.put("bootstrap.servers", config.getKafkaUrl());

            String currentDir = System.getProperty("user.dir");
            FileUtil.WriteProperties(currentDir + "/conf/" + "kafka_producer.properties",
                    updateProps, dirprmPath + "/" + config.getKafkaProducerName(), bw);

            writeAndPrint("****************************** KAFKA PRODUCER DEPLOY SUCCESS ********************************");

        } catch (Exception e) {
            writeAndPrint("******************************** KAFKA PRODUCER DEPLOY FAIL *********************************");
            throw e;
        }

    }
}
