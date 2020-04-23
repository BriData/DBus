package com.creditease.dbus.router.bolt.manager;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.PropertiesUtils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerManager extends KafkaClientManager<KafkaProducer<String, byte[]>> {

    private static Logger logger = LoggerFactory.getLogger(KafkaProducerManager.class);

    public KafkaProducerManager(Properties properties, String securityConf) {
        super(properties, securityConf);
    }

    public void close() {
        super.close();
        kafkaClientCache.foreach((key, value) -> {((KafkaProducer) value).close(); return false;});
        kafkaClientCache.clear();
    }

    @Override
    public KafkaProducer<String, byte[]> obtainKafkaClient(String url) {
        KafkaProducer<String, byte[]> producer = null;
        try {
            Properties props = Optional.ofNullable(PropertiesUtils.copy(properties)).orElseGet(() -> {return new Properties();});
            props.put("bootstrap.servers", url);
            if (properties.contains("client.id"))
                properties.remove("client.id");
            if (Objects.equals(Constants.SECURITY_CONFIG_TRUE_VALUE, securityConf))
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            producer = new KafkaProducer<>(props);
            logger.info("producer manager create kafka producer. url:{}", url);
        } catch (Exception e) {
            logger.error("producer manager create kafka producer error. url:{}", url);
            throw new RuntimeException(e);
        }
        return producer;
    }

}
