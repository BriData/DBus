package com.creditease.dbus.router.spout.handler;

import java.util.Objects;

import com.creditease.dbus.router.spout.aware.KafkaConsumerAware;
import com.creditease.dbus.router.spout.handler.processor.MonitorSpoutProcessorChain;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerRecordsHandler implements Handler, KafkaConsumerAware {

    private MonitorSpoutProcessorChain chain = null;
    private KafkaConsumer kafkaConsumer = null;

    public ConsumerRecordsHandler(MonitorSpoutProcessorChain chain) {
        this.chain = chain;
    }

    @Override
    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.chain.setKafkaConsumer(kafkaConsumer);
    }

    @Override
    public Object handle(Object obj) {
        Objects.requireNonNull(obj, "consumer records is not null");
        boolean isBreak = false;
        for (Object record : (ConsumerRecords) obj)
            if (isBreak = (Boolean) chain.process(record))
                break;
        return isBreak;
    }

}
