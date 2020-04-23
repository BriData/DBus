package com.creditease.dbus.router.spout.aware;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaConsumerAware {

    void setKafkaConsumer(KafkaConsumer kafkaConsumer);

}
