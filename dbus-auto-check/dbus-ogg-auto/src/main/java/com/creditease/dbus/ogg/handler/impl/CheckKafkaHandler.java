package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * User: 王少楠
 * Date: 2018-08-27
 * Desc:
 */
public class CheckKafkaHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        checkKafka(bw);
    }

    private void checkKafka(BufferedWriter bw) throws Exception{
        ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
        String kafkaUrl = config.getKafkaUrl();
        String[] brokers = kafkaUrl.split(",");
        System.out.println("============================================");
        System.out.println("检测kafka url: [kafka url ="+kafkaUrl+"]");
        bw.write("============================================");
        bw.newLine();
        bw.write("检测kafka url: [kafka url ="+kafkaUrl+"]");
        bw.newLine();


        for(int i = 0; i < brokers.length; i++) {
            String arr[] = StringUtils.split(brokers[i], ":");
            try{
                testKafkaConn(bw, arr[0], arr[1]);
            }catch (Exception e){
                bw.write("检测异常，请检查kafka配置项！");
                bw.newLine();
                System.out.println("检测异常，请检查kafka配置项！");

                throw e;
            }
        }

    }

    private void testKafkaConn(BufferedWriter bw,String ip ,String port) throws Exception{
        Socket socket = null;
        try{
            socket = new Socket();
            socket.connect(new InetSocketAddress(ip,Integer.valueOf(port)));
            socket.close();
            System.out.println("kafka 连接正常：[ 地址: "+ip+"  端口："+port+"]");
            bw.write("kafka 连接正常：[ 地址: "+ip+"  端口："+port+"]");
            bw.newLine();
        } catch (Exception e) {
            System.out.println("kafka 连接异常：[ 地址: "+ip+"  端口："+port+"]");
            bw.write("kafka 连接异常：[ 地址: "+ip+"  端口："+port+"]");
            bw.newLine();
            throw e;
        }finally {
            if(socket!=null)
                socket.close();
        }
    }

}
