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
    public void check(BufferedWriter bw) throws Exception {
          checkKafka(bw);
    }

    @Override
    public void deploy(BufferedWriter bw) throws Exception {
    }

    public void checkKafka(BufferedWriter bw) throws Exception {

        LogCheckConfigBean lccb = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String kafkaBroker = lccb.getKafkaBootstrapServers();
        String []kafkaList = StringUtils.split(kafkaBroker, ",");

        for(int i = 0; i < kafkaList.length; i++) {
            String arr[] = StringUtils.split(kafkaList[i], ":");
            testKafkaConn(bw, arr[0], arr[1]);
        }
    }

    private void testKafkaConn(BufferedWriter bw, String kafkaAddr, String port) throws IOException {
        boolean kafkaTestResult = true;
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(kafkaAddr, Integer.parseInt(port)));
            socket.close();
            bw.write("kafka 连接正常: [kafka 地址： " + kafkaAddr + "  端口： " + port +"]\n");
        } catch (IOException e) {
            logger.error("连通性测试异常. errorMessage:{};host:{},port:{}", e.getMessage(), kafkaAddr, port, e);
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
    }
}
