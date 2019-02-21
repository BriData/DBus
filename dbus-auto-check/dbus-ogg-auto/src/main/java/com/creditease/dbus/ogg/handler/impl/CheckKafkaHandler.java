package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-27
 * Desc:
 */
public class CheckKafkaHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        checkKafka(bw);
    }

    private void checkKafka(BufferedWriter bw) throws Exception {
        ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
        String kafkaUrl = config.getKafkaUrl();
        String[] brokers = kafkaUrl.split(",");
        writeAndPrint("********************************** CHECK KAFKA URL START *************************************");
        writeAndPrint("检测kafka url: [kafka url =" + kafkaUrl + "]");

        for (int i = 0; i < brokers.length; i++) {
            String arr[] = StringUtils.split(brokers[i], ":");
            try {
                testKafkaConn(arr[0], arr[1]);
            } catch (Exception e) {
                writeAndPrint("检测kafka url异常，请检查kafka配置！");
                writeAndPrint("********************************* CHECK KAFKA URL FAIL **************************************");
                throw e;
            }
        }
        writeAndPrint("******************************* CHECK KAFKA URL SUCCDESS ************************************");
    }

    private void testKafkaConn(String ip, String port) throws Exception {
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(ip, Integer.valueOf(port)));
            socket.close();
            writeAndPrint("kafka 连接正常：[ 地址: " + ip + "  端口：" + port + "]");
        } catch (Exception e) {
            writeAndPrint("kafka 连接异常：[ 地址: " + ip + "  端口：" + port + "]");
            throw e;
        } finally {
            if (socket != null)
                socket.close();
        }
    }

}
