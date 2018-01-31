package com.creditease.dbus.ws.tools;

import com.creditease.dbus.mgr.base.ConfUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by haowei6 on 2017/10/19.
 */
public class PlainLogKafkaConsumerProvider {
    private static KafkaConsumer<String, String> consumer = null;
    public static KafkaConsumer<String, String> getKafkaConsumer() throws Exception {
        if(consumer == null) {
            synchronized (PlainLogKafkaConsumerProvider.class) {
                if(consumer == null) {
                    Properties consumerProps = ConfUtils.load("consumer.properties");
                    consumerProps.setProperty("client.id","plain.log.reader");
                    consumerProps.setProperty("group.id","plain.log.reader");
                    ZookeeperServiceProvider zk = ZookeeperServiceProvider.getInstance();
                    Properties globalConf = zk.getZkService().getProperties(com.creditease.dbus.ws.common.Constants.GLOBAL_CONF);
                    consumerProps.setProperty("bootstrap.servers", globalConf.getProperty("bootstrap.servers"));
                    consumer = new KafkaConsumer(consumerProps);
                }
            }
        }
        return consumer;
    }
}
