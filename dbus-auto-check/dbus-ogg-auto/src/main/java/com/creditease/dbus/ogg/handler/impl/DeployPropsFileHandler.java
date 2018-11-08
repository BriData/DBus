package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.FileUtil;

import java.io.*;
import java.util.Properties;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class DeployPropsFileHandler extends AbstractHandler{
    @Override
    public void checkDeploy(BufferedWriter bw)throws Exception {
        deployProducerProps(bw);
        deployReplicateProps(bw);
    }

    private void deployReplicateProps(BufferedWriter bw) throws Exception{
        try {
            ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
            String dsName = config.getDsName();
            //dirprm的目录：如 /u01/golden123111/dirprm/
            String dirprmPath = config.getOggBigHome() + "/dirprm";
            System.out.println("============================================");
            System.out.println("开始部署replicat进程的配置文件  " + dsName + ".props");
            bw.write("============================================");
            bw.newLine();
            bw.write("开始部署replicat进程的配置文件  " + dsName + ".props");
            bw.newLine();

            Properties updateProps = new Properties();
            updateProps.put("gg.handler.kafkahandler.KafkaProducerConfigFile",
                    config.getKafkaProducerName());
            updateProps.put("gg.handler.kafkahandler.topicMappingTemplate", dsName);
            updateProps.put("gg.handler.kafkahandler.schemaTopicName", dsName + "_schema");


            String currentDir = System.getProperty("user.dir");
            FileUtil.WriteProperties(currentDir + "/conf/" + "replicate.properties",
                    updateProps, dirprmPath + "/" + dsName + ".props",bw);

            System.out.println("部署完毕");
            bw.write("部署完毕");
            bw.newLine();

        }catch (Exception e){
            System.out.println("部署失败！！！！");
            bw.write("部署失败！！！！");
            bw.newLine();
            throw e;
        }


    }

    private void deployProducerProps(BufferedWriter bw) throws Exception{
        try {
            ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();

            System.out.println("============================================");
            System.out.println("开始部署kafka producer的配置文件  " + config.getKafkaProducerName());
            bw.write("============================================");
            bw.newLine();
            bw.write("开始部署kafka producer的配置文件  " + config.getKafkaProducerName());
            bw.newLine();

            //dirprm的目录：如 /u01/golden123111/dirprm/
            String dirprmPath = config.getOggBigHome() + "/dirprm";
            File path = new File(dirprmPath);
            if(!path.exists()){
                System.out.println(dirprmPath+"  目录不存在，请检查ogg.big.home 配置是否正确");
                bw.write(dirprmPath+"  目录不存在，请检查ogg.big.home 配置是否正确");
                bw.newLine();
                throw  new FileNotFoundException();
            }

            Properties updateProps = new Properties();
            updateProps.put("bootstrap.servers", config.getKafkaUrl());

            String currentDir = System.getProperty("user.dir");
            FileUtil.WriteProperties(currentDir + "/conf/" + "kafka_producer.properties",
                    updateProps, dirprmPath + "/" + config.getKafkaProducerName(),bw);

            System.out.println("部署完毕");
            bw.write("部署完毕");
            bw.newLine();

        }catch (Exception e){
            System.out.println("kafka producer的配置文件部署失败！！！！");
            bw.write("kafka producer的配置文件部署失败！！！！");
            bw.newLine();
            throw e;
        }

    }
}
