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


package com.creditease.dbus.log.handler.impl;


import com.creditease.dbus.log.bean.LogCheckConfigBean;
import com.creditease.dbus.log.container.AutoCheckConfigContainer;
import com.creditease.dbus.log.handler.AbstractHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;


public class CheckKafkaHandler extends AbstractHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void checkDeploy(BufferedWriter bw) throws Exception {
        checkKafka(bw);
    }

    public void checkKafka(BufferedWriter bw) throws Exception {
        LogCheckConfigBean lccb = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String kafkaBroker = lccb.getKafkaBootstrapServers();
        String[] kafkaList = StringUtils.split(kafkaBroker, ",");

        boolean result = true;
        for (int i = 0; i < kafkaList.length; i++) {
            String arr[] = StringUtils.split(kafkaList[i], ":");
            if (testKafkaConn(bw, arr[0], arr[1]) == false)
                result = false;
        }

        if (!result) {
            bw.write("检测异常，请检查kafka配置项！\n");
            bw.flush();
            bw.close();
            System.exit(0);
        }

    }

    private boolean testKafkaConn(BufferedWriter bw, String kafkaAddr, String port) throws IOException {
        boolean kafkaTestResult = true;
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(kafkaAddr, Integer.parseInt(port)));
            socket.close();
            bw.write("kafka 连接正常: [kafka 地址： " + kafkaAddr + "  端口： " + port + "]\n");
            System.out.println("kafka 连接正常: [kafka 地址： " + kafkaAddr + "  端口： " + port + "]");
        } catch (IOException e) {
            System.out.println("kafka 连接超时，请检查kafka配置!\n " + "[host: " + kafkaAddr + "port: " + port + "]");
            bw.write("kafka 连接超时，请检查kafka配置！\n ERROR: " + e.getMessage() + "\n");
            kafkaTestResult = false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return kafkaTestResult;
    }
}
