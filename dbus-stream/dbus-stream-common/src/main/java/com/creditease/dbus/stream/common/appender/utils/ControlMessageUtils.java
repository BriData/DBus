package com.creditease.dbus.stream.common.appender.utils;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ControlMessageUtils {

    private static Logger logger = LoggerFactory.getLogger(ControlMessageUtils.class);

    public static void buildAndSendControlMessage(String from, String subject, String contents) {
        ControlMessage controlMessage = new ControlMessage(System.currentTimeMillis(), ControlType.COMMON_EMAIL_MESSAGE.toString(), from);
        controlMessage.addPayload("subject", subject);
        controlMessage.addPayload("contents", contents);
        controlMessage.addPayload("datasource", Utils.getDatasource().getDsName());
        ControlMessageUtils.sendControlMessage(controlMessage);
    }

    public static void sendControlMessage(ControlMessage controlMessage) {
        Producer<String, String> producer = null;
        try {
            // 发邮件
            Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
            props.setProperty("client.id", ControlMessageUtils.class.getName() + "." + System.nanoTime());
            producer = new KafkaProducer<>(props);

            String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.GLOBAL_EVENT_TOPIC);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, controlMessage.getType(), controlMessage.toJSONString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Send global event error.{}", exception.getMessage());
                }
            });
        } catch (Exception e1) {
            logger.error("exception data process error.", e1);
        } finally {
            if (producer != null) producer.close();
        }
    }
}
