package com.creditease.dbus.router.bolt.manager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.PropertiesUtils;
import com.creditease.dbus.router.cache.function.EachFunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerManager extends KafkaClientManager<KafkaConsumer<String, byte[]>> {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerManager.class);
    private String suffix = StringUtils.EMPTY;

    public KafkaConsumerManager(Properties properties, String securityConf, String suffix) {
        super(properties, securityConf);
        this.suffix = suffix;
    }

    public void close() {
        super.close();
        kafkaClientCache.foreach((key, value) -> {((KafkaConsumer) value).close(); return false;});
        kafkaClientCache.clear();
    }

    public void subscribe(String url, String topic, boolean isCtrlTopic) {
        Optional.ofNullable(getKafkaClient(url).getValue()).ifPresent(consumer -> {
            KafkaConsumer kafkaConsumer = (KafkaConsumer) consumer;
            Set<TopicPartition> subscription = new HashSet<TopicPartition>(kafkaConsumer.assignment());
            List partitions = new ArrayList<PartitionInfo>(kafkaConsumer.partitionsFor(topic));
            partitions.stream().map(partitionInfo -> new TopicPartition(((PartitionInfo) partitionInfo).topic(), ((PartitionInfo) partitionInfo).partition()));
            subscription.addAll(partitions);
            kafkaConsumer.assign(subscription);
            subscription.forEach(topicPartition -> logger.info("subscriptions: {}", topicPartition.toString()));
            if (isCtrlTopic)
                kafkaConsumer.seekToEnd(partitions);
        });
    }

    @Override
    public KafkaConsumer<String, byte[]> obtainKafkaClient(String url) {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            Properties props = Optional.ofNullable(PropertiesUtils.copy(properties)).orElseGet(() -> {return new Properties();});
            props.setProperty("bootstrap.servers", url);
            props.setProperty("group.id", StringUtils.joinWith("-", props.getProperty("group.id"), suffix));
            props.setProperty("client.id", StringUtils.joinWith("-", props.getProperty("client.id"), suffix));
            if (StringUtils.equals(securityConf, Constants.SECURITY_CONFIG_TRUE_VALUE))
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            consumer = new KafkaConsumer<>(props);
            logger.info("consumer manager create kafka consumer. url:{}", url);
        } catch (Exception e) {
            logger.error("consumer manager create kafka consumer error. url:{}", url);
            throw new RuntimeException(e);
        }
        return consumer;
    }

    public void foreach(EachFunction func) {
        kafkaClientCache.foreach(func);
    }

}
